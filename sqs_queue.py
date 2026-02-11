import json
from datetime import datetime, timezone
from logging import getLogger
from signal import SIGTERM, getsignal, signal
from time import sleep

import boto3
from cast_from_env import from_env

logger = getLogger(__name__)


class Queue(object):
    got_sigterm = False

    def __init__(self, queue_name=None, queue=None, bulk_queue=None, poll_wait=20, poll_sleep=40,
                 sns=False, drain=False, batch=True, trap_sigterm=True, endpoint_url=None,
                 create=False, **kwargs):

        if not queue_name and not queue:
            raise ValueError('Must provide "queue" resource or "queue_name" parameter')

        if queue_name:
            sqs = boto3.resource('sqs', endpoint_url=endpoint_url)
            try:
                queue = sqs.get_queue_by_name(QueueName=queue_name)
            except sqs.meta.client.exceptions.QueueDoesNotExist:
                if not create:
                    raise
                queue = sqs.create_queue(QueueName=queue_name, **kwargs)

        self.queue = queue
        self.bulk_queue = bulk_queue

        self.set_from_env('poll_wait', poll_wait)
        self.set_from_env('poll_sleep', poll_sleep)
        self.set_from_env('sns', sns)
        self.set_from_env('drain', drain)
        self.set_from_env('batch', batch)

        if trap_sigterm:
            signal(SIGTERM, self.make_sigterm_handler())

    def set_from_env(self, var, default):
        value = from_env(f'SQS_QUEUE_{var}'.upper(), default)
        setattr(self, var.lower(), value)

    def __iter__(self):
        self.consumer = self.queue_consumer()
        return self.consumer

    def queue_consumer(self):
        while not self.got_sigterm:
            max_count = 10 if self.batch else 1
            logger.debug('Receiving messages from queue queue_url=%s, max_count=%s, wait_time=%ds',
                         self.queue.url, max_count, self.poll_wait)
            messages = self.receive(max_count, wait=self.poll_wait)
            logger.debug('Received messages from queue queue_url=%s, message_count=%d',
                         self.queue.url, len(messages))

            unprocessed = yield from self._process_messages(messages)

            if unprocessed:
                logger.info('Putting messages back in queue message_count=%d', len(unprocessed))
                entries = [
                    {'Id': str(i), 'ReceiptHandle': handle, 'VisibilityTimeout': 0}
                    for i, handle in enumerate(unprocessed)
                ]
                self.queue.change_message_visibility_batch(Entries=entries)

            if not messages:
                if self.bulk_queue:
                    logger.debug('Primary queue empty, checking bulk queue')
                    bulk_messages = self.bulk_queue.receive(max_count)
                    if bulk_messages:
                        logger.info('Received %d messages from bulk queue', len(bulk_messages))
                        yield from self._process_messages(bulk_messages)
                        continue
                if self.drain:
                    return
                logger.debug('Sleeping between polls poll_sleep=%ds', self.poll_sleep)
                sleep(self.poll_sleep)

        logger.info('Got SIGTERM, exiting')

    def _process_messages(self, messages):
        """Yield messages to consumer, handle SNS unwrapping and deletion.

        Returns list of unprocessed receipt handles (for SIGTERM handling).
        """
        unprocessed = []
        for message in messages:
            sqs_message = message.sqs_message
            logger.debug(
                'Processing SQS message_id=%s, '
                'sent_at=%s, first_received_at=%s, '
                'receive_count=%s, message_group_id=%s',
                sqs_message.message_id,
                utc_from_timestamp(sqs_message, 'SentTimestamp'),
                utc_from_timestamp(sqs_message, 'ApproximateFirstReceiveTimestamp'),
                sqs_message.attributes.get('ApproximateReceiveCount'),
                sqs_message.attributes.get('MessageGroupId')
            )
            if self.got_sigterm:
                unprocessed.append(sqs_message.receipt_handle)
                continue

            if self.sns and not self._unwrap_sns(message):
                continue

            leave_in_queue = yield message
            if leave_in_queue:
                logger.debug('Leaving SQS message in queue message_id=%s',
                             sqs_message.message_id)
                yield
            else:
                self._delete_message(message)

        return unprocessed

    def _unwrap_sns(self, message):
        """Unwrap SNS message envelope in place. Returns True on success."""
        try:
            sns_message_id = message['MessageId']
            sns_sequence_number = message.get('SequenceNumber')
            sns_timestamp = message.get('Timestamp')
            inner_body = json.loads(message['Message'])
            message.clear()
            message.update(inner_body)
            message['sns_message_id'] = sns_message_id
            message['sns_sequence_number'] = sns_sequence_number
            message['sns_timestamp'] = sns_timestamp
            return True
        except ValueError:
            logger.warning('SNS "Message" in SQS message body is not valid JSON, skipping'
                           'message_id=%s', message.sqs_message.message_id)
            return False
        except KeyError as e:
            logger.warning('SQS message JSON is missing required key, skipping key=%s '
                           'message_id=%s', e, message.sqs_message.message_id)
            return False

    def _delete_message(self, message):
        """Delete a message from its queue."""
        sqs_message = message.sqs_message
        logger.debug('Deleting SQS message from queue message_id=%s', sqs_message.message_id)
        try:
            sqs_message.delete()
        except Exception as e:
            logger.warning('Unable to delete SQS message_id=%s, error=%s',
                           sqs_message.message_id, e)

    def receive(self, max_count=10, wait=0):
        """Receive up to max_count messages from the queue.

        Args:
            max_count: Maximum number of messages to receive (1-10).
            wait: Seconds to wait for messages (0 for non-blocking).
        """
        sqs_messages = self.queue.receive_messages(
            MaxNumberOfMessages=max_count,
            WaitTimeSeconds=wait,
            MessageAttributeNames=['All'],
            AttributeNames=['All']
        )
        messages = []
        for sqs_message in sqs_messages:
            body = self._parse_json(sqs_message)
            if body is not None:
                messages.append(Message(body, self, sqs_message))
        return messages

    def _parse_json(self, sqs_message):
        """Parse JSON body from SQS message. Returns None if invalid."""
        try:
            return json.loads(sqs_message.body)
        except ValueError:
            logger.warning('SQS message body is not valid JSON, skipping message_id=%s',
                           sqs_message.message_id)
            return None

    def publish(self, body, **kwargs):
        if isinstance(body, dict):
            body = json.dumps(body)
        self.queue.send_message(MessageBody=body, **kwargs)

    def make_sigterm_handler(self):
        existing_handler = getsignal(SIGTERM)

        def set_terminate_flag(signum, frame):
            logger.info('Got SIGTERM, will exit after this batch')
            self.got_sigterm = True
            if callable(existing_handler):
                existing_handler(signum, frame)

        return set_terminate_flag


class Message(dict):

    def __init__(self, body, queue, sqs_message=None):
        dict.__init__(self)
        self.update(body)
        self.queue = queue
        self.sqs_message = sqs_message

    def defer(self):
        self.queue.consumer.send(True)


def utc_from_timestamp(message, attribute):
    ts = message.attributes.get(attribute)
    return datetime.fromtimestamp(int(ts) / 1000, timezone.utc) if ts else None
