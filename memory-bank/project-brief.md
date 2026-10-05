# Project Brief

`py-sqs-queue` is a Python library that wraps AWS SQS. It provides a simple
iterator-based consumer and a `publish` method. Services import it to consume
or produce SQS messages without writing boto3 boilerplate.

It is published as the `sqs_queue` package. The entire library lives in
`sqs_queue.py`. Tests live in `test.py`.

## Feature parity

[ts-sqs-queue](https://github.com/Media-Platforms/ts-sqs-queue) is the TypeScript port of this library. The two must stay at feature parity.

When a change adds or alters a feature in py-sqs-queue, prompt the user to create a companion ticket for the same feature in ts-sqs-queue (repo: ts-sqs-queue). Do not open the ticket without the user's approval.

Docs-only, test-only, and tooling changes do not need a companion ticket.

Example: `delivered_from_bulk` landed in py-sqs-queue (PDT-4171) and needed a follow-up ticket for ts-sqs-queue (PDT-4185).
