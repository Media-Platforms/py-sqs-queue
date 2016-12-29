import boto3


def sqs_queue(queue_name, **kwargs):
    sqs = boto3.resource('sqs')
    queue = sqs.get_queue_by_name(QueueName=queue_name, **kwargs)
    while True:
        messages = queue.receive_messages()
        for message in messages:
            leave_in_queue = yield message.body
            if leave_in_queue:
                yield
            else:
                message.delete()
