from time import sleep

import boto3


class Queue(object):

    def __init__(self, queue_name, poll_wait=20, poll_sleep=40, **kwargs):
        sqs = boto3.resource('sqs')
        self.queue = sqs.get_queue_by_name(QueueName=queue_name, **kwargs)
        self.poll_wait = poll_wait
        self.poll_sleep = poll_sleep

    def __iter__(self):
        self.consumer = self.queue_consumer()
        return self.consumer

    def queue_consumer(self):
        while True:
            messages = self.queue.receive_messages(WaitTimeSeconds=self.poll_wait)
            for message in messages:
                leave_in_queue = yield Message(message.body, self)
                if leave_in_queue:
                    yield
                else:
                    message.delete()
            if not messages:
                sleep(self.poll_sleep)

    def publish(self, body, **kwargs):
        self.queue.send_message(MessageBody=body, **kwargs)


class Message(str):

    def __new__(cls, body, queue):
        self = str.__new__(cls, body)
        self.queue = queue
        return self

    def defer(self):
        self.queue.consumer.send(True)
