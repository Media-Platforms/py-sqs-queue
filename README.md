# py-sqs-queue

Simple Python AWS SQS queue consumer and publisher

## Installation

`python setup.py install`

## Examples

    from sqs_queue import Queue

    my_queue = Queue('YOUR_QUEUE_NAME')
    for message in my_queue:
        process(message)

Or, if you'd like to leave unprocessable messages in the queue to be retried again later:

    for message in my_queue:
        try:
            process(message)
        except RetryableError:
            message.defer()
        except Exception as e:
            logger.warn(e)

And, you can publish to the queue as well:

    queue.publish('This is the body of my message.')

## Parameters

Behind the scenes, the generator is polling SQS for new messages. When the queue is empty, that
call will wait up to 20 seconds for new messages, and if it times out before any arrive it will
sleep for 40 seconds before trying again. Those time intervals are configurable:

    queue = Queue('YOUR_QUEUE_NAME', poll_wait=20, poll_sleep=40)
