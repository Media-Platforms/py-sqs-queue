# Project Brief

`py-sqs-queue` is a Python library that wraps AWS SQS. It provides a simple
iterator-based consumer and a `publish` method. Services import it to consume
or produce SQS messages without writing boto3 boilerplate.

It is published as the `sqs_queue` package. The entire library lives in
`sqs_queue.py`. Tests live in `test.py`.
