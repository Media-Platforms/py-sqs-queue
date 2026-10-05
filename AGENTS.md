# Agent instructions

Before editing Python in this repository, read [memory-bank/coding-style.md](memory-bank/coding-style.md).

Line length is **100** characters. Keep each statement on one line when it still fits within that limit; wrap only when a single line would exceed 100 columns.

## Feature parity

[ts-sqs-queue](https://github.com/Media-Platforms/ts-sqs-queue) is the TypeScript port of this library. The two must stay at feature parity.

When a change adds or alters a feature in py-sqs-queue, prompt the user to create a companion ticket for the same feature in ts-sqs-queue (repo: ts-sqs-queue). Do not open the ticket without the user's approval.

Docs-only, test-only, and tooling changes do not need a companion ticket.

Example: `delivered_from_bulk` landed in py-sqs-queue (PDT-4171) and needed a follow-up ticket for ts-sqs-queue (PDT-4185).
