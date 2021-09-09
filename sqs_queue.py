import json
from datetime import datetime
from logging import getLogger
from signal import SIGTERM, getsignal, signal
from time import sleep

import boto3

logger = getLogger(__name__)

def utc_from_timestamp(message, attribute):
    ts = message.attributes.get(attribute)
    return datetime.utcfromtimestamp(int(ts) / 1000) if ts else None


class Queue(object):
    got_sigterm = False

    def __init__(self, queue_name=None, queue=None, poll_wait=20, poll_sleep=40, sns=False,
                 drain=False, batch=True, trap_sigterm=True, endpoint_url=None, **kwargs):
        if not queue_name and not queue:
            raise ValueError('Must provide "queue" resource or "queue_name" parameter')
        if queue_name:
            sqs = boto3.resource('sqs', endpoint_url=endpoint_url)
            queue = sqs.get_queue_by_name(QueueName=queue_name, **kwargs)
        self.queue = queue
        self.poll_wait = poll_wait
        self.poll_sleep = poll_sleep
        self.sns = sns
        self.drain = drain
        self.batch = batch

        if trap_sigterm:
            signal(SIGTERM, self.make_sigterm_handler())

    def __iter__(self):
        self.consumer = self.queue_consumer()
        return self.consumer

    def queue_consumer(self):
        while not self.got_sigterm:
            max_count = 10 if self.batch else 1
            logger.debug('Receiving messages from queue %s (max count: %s, wait time: %ds)',
                         self.queue.url, max_count, self.poll_wait)
            messages = self.queue.receive_messages(
                MaxNumberOfMessages=max_count,
                WaitTimeSeconds=self.poll_wait,
                MessageAttributeNames=['All'],
                AttributeNames=['All']
            )
            logger.info('Received %d messages from queue %s ', len(messages), self.queue.url)

            unprocessed = []

            for message in messages:
                logger.debug(
                    'Processing SQS message ID "%s" '
                    '(sent at: %s, first received at: %s, '
                    'receive count: %s, message group ID: %s)',
                    message.message_id,
                    utc_from_timestamp(message, 'SentTimestamp'),
                    utc_from_timestamp(message, 'ApproximateFirstReceiveTimestamp'),
                    message.attributes.get('ApproximateReceiveCount'),
                    message.attributes.get('MessageGroupId')
                )
                if self.got_sigterm:
                    unprocessed.append(message.receipt_handle)
                    continue

                try:
                    body = json.loads(message.body)
                except ValueError:
                    logger.warning('SQS message body is not valid JSON, skipping')
                    continue

                if self.sns:
                    try:
                        message_id = body['MessageId']
                        sequence_number = body.get('SequenceNumber')
                        timestamp = body.get('Timestamp')
                        body = json.loads(body['Message'])
                        body['sns_message_id'] = message_id
                        body['sns_sequence_number'] = sequence_number
                        body['sns_timestamp'] = timestamp
                    except ValueError:
                        logger.warning('SNS "Message" in SQS message body is not valid JSON, skipping')
                        continue
                    except KeyError as e:
                        logger.warning('SQS message JSON has no "%s" key, skipping', e)
                        continue

                leave_in_queue = yield Message(body, self, message)
                if leave_in_queue:
                    logger.debug('Leaving SQS message "%s" in queue', message.message_id)
                    yield
                else:
                    logger.debug('Deleting SQS message "%s" from queue', message.message_id)
                    message.delete()

            if not messages:
                if self.drain:
                    return
                logger.debug('Sleeping for %ds between polls', self.poll_sleep)
                sleep(self.poll_sleep)

            if unprocessed:
                logger.info('Putting %s messages back in queue', len(unprocessed))
                entries = [
                    {'Id': str(i), 'ReceiptHandle': handle, 'VisibilityTimeout': 0}
                    for i, handle in enumerate(unprocessed)
                ]
                self.queue.change_message_visibility_batch(Entries=entries)

        logger.info('Got SIGTERM, exiting')

    def publish(self, body, **kwargs):
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
