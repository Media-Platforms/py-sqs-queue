# py-sqs-consumer

Simple Python AWS SQS queue consumer

## Installation

`python setup.py install`

## Examples

    from sqs_consumer import sqs_queue

    queue = sqs_queue('YOUR_QUEUE_NAME')
    for message in queue:
        process(message)

Or, if you'd like to leave unprocessable messages in the queue to be retried again later:

    for message in queue:
        try:
            process(message)
        except RetryableError:
            queue.send(True)
        except Exception as e:
            logger.warn(e)

## Parameters

Behind the scenes, the generator is polling SQS for new messages. When the queue is empty, that
call will wait up to 20 seconds for new messages, and if it times out before any arrive it will
sleep for 40 seconds before trying again. Those time intervals are configurable:

    queue = sqs_queue('YOUR_QUEUE_NAME', poll_wait=20, poll_sleep=40)
