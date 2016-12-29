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
