# py-sqs-queue

Simple Python AWS SQS queue consumer and publisher

## Installation

`python setup.py install`

## Examples

    from sqs_queue import Queue

    my_queue = Queue('YOUR_QUEUE_NAME')
    for message in my_queue:
        your_process_fn(message)

Or, if you'd like to leave unprocessable messages in the queue to be retried again later:

    for message in my_queue:
        try:
            your_process_fn(message)
        except YourRetryableError:
            message.defer()
        except Exception as e:
            logger.warn(e)

And, you can publish to the queue as well:

```py
queue.publish({'MessageId': 123, 'Message': '{"foo": "bar"}'})
```

If you already have a boto3 queue resource, pass this instead of a name:

```py
import boto3
from sqs_queue import Queue

queue_resource = boto3.resource('sqs').Queue('YOUR_QUEUE_NAME')

my_queue = Queue(queue=queue_resource)
```

## Configuration

You can put your AWS credentials in environment variables or [any of the other places boto3 looks](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html).

## Parameters


### `poll_wait` and `poll_sleep`

Behind the scenes, the generator is polling SQS for new messages. When the queue is empty, that
call will wait up to 20 seconds for new messages, and if it times out before any arrive it will
sleep for 40 seconds before trying again. Those time intervals are configurable:

```py
queue = Queue('YOUR_QUEUE_NAME', poll_wait=20, poll_sleep=40)
```

### `drain`

Normally, once the queue is empty, the generator waits for more messages. If you just want to process all existing messages and quit, you can pass this boolean parameter:

```py
queue = Queue('YOUR_QUEUE_NAME', drain=True)
```

For example, if your queue is long and your consumers are falling behind, you can start a bunch of consumers with `drain=True` and they'll quit when you've caught up.

### `sns`

If your SQS queue is being fed from an SNS topic, you can pass your Queue this boolean parameter, and then your messages will just contain the SNS notification data, so you don't have to fish it out of the SQS message and decode it:

```py
queue = Queue('YOUR_QUEUE_NAME', sns=True)
```
When you use this option, the `sns_message_id` is added to the notification data, which can be used to make sure you only process each message once.
