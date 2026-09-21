# Coding style

## Line length

- Max line length is **100** characters.
- Prefer filling up to that limit. Do **not** wrap early when the call or
  expression still fits on one line within 100 columns.
- Example (preferred):

```python
bulk_messages = self.bulk_queue.receive(max_count, consumer_queue=self)
logger.info('Received %d messages from bulk queue', len(bulk_messages))
yield from self._process_messages(bulk_messages)
```

- Wrap only when a single line would exceed 100 characters, or when
  multi-line form is required for readability of large dict/kwarg blocks
  that already follow that pattern in the file.
