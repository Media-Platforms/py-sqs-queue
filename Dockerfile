FROM python:3.13-slim

WORKDIR /app

COPY setup.py .
COPY sqs_queue.py .
COPY README.md .

RUN pip install -e . ipython

CMD ["ipython"]
