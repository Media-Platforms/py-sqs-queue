from time import sleep

import boto3


def sqs_queue(queue_name, poll_wait=20, poll_sleep=40, **kwargs):
    sqs = boto3.resource('sqs')
    queue = sqs.get_queue_by_name(QueueName=queue_name, **kwargs)
    while True:
        messages = queue.receive_messages(WaitTimeSeconds=poll_wait)
        for message in messages:
            leave_in_queue = yield message.body
            if leave_in_queue:
                yield
            else:
                message.delete()
        if not messages:
            sleep(poll_sleep)
