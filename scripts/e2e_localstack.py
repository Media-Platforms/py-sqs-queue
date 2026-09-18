#!/usr/bin/env python3
"""Publish and consume one message against LocalStack SQS."""
import json
import os
import sys

from sqs_queue import Queue

ENDPOINT = os.environ.get("AWS_ENDPOINT_URL", "http://127.0.0.1:4566")
QUEUE_NAME = "py-sqs-queue-e2e"


def main() -> int:
    os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
    os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")
    os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")

    queue = Queue(
        queue_name=QUEUE_NAME,
        create=True,
        endpoint_url=ENDPOINT,
        drain=True,
        poll_wait=2,
    )
    payload = {"hello": "cloud-agent", "n": 1}
    queue.publish(payload)

    received = []
    for message in queue:
        received.append(dict(message))

    if len(received) != 1:
        print(f"Expected 1 message, got {len(received)}", file=sys.stderr)
        return 1
    if received[0] != payload:
        print(
            f"Payload mismatch: {json.dumps(received[0])} != {json.dumps(payload)}",
            file=sys.stderr,
        )
        return 1
    print(json.dumps({"status": "ok", "message": received[0]}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
