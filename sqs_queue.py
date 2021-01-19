import json
from logging import getLogger
from time import sleep

import boto3

logger = getLogger(__name__)


class Queue(object):

    def __init__(self, queue_name, poll_wait=20, poll_sleep=40, sns=False, drain=False, **kwargs):
        sqs = boto3.resource('sqs')
        self.queue = sqs.get_queue_by_name(QueueName=queue_name, **kwargs)
        self.poll_wait = poll_wait
        self.poll_sleep = poll_sleep
        self.sns = sns
        self.drain = drain

    def __iter__(self):
        self.consumer = self.queue_consumer()
        return self.consumer

    def queue_consumer(self):
        while True:
            messages = self.queue.receive_messages(WaitTimeSeconds=self.poll_wait)
            for message in messages:
                try:
                    body = json.loads(message.body)
                except ValueError:
                    logger.warn('SQS message body is not valid JSON, skipping')
                    continue
                if self.sns:
                    try:
                        message_id = body['MessageId']
                        body = json.loads(body['Message'])
                        body['sns_message_id'] = message_id
                    except ValueError:
                        logger.warn('SNS "Message" in SQS message body is not valid JSON, skipping')
                        continue
                    except KeyError as e:
                        logger.warn('SQS message JSON has no "%s" key, skipping', e)
                        continue
                leave_in_queue = yield Message(body, self)
                if leave_in_queue:
                    yield
                else:
                    message.delete()
            if not messages:
                if self.drain:
                    return
                sleep(self.poll_sleep)

    def publish(self, body, **kwargs):
        self.queue.send_message(MessageBody=body, **kwargs)


class Message(dict):

    def __init__(self, body, queue):
        dict.__init__(self)
        self.update(body)
        self.queue = queue

    def defer(self):
        self.queue.consumer.send(True)
