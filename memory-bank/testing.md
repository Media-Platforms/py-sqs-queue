# Running Tests

## Test stack

- Framework: Python `unittest` (stdlib), executed via **pytest**.
- Coverage: **pytest-cov** / **coverage** with a 100% gate on `sqs_queue.py` (see `.coveragerc`).
- All AWS calls are mocked. No localstack, no docker, no AWS credentials needed.

## One-time setup (local dev)

```bash
cd /path/to/py-sqs-queue
python3 -m venv ../sqs-venv
source ../sqs-venv/bin/activate
pip install -U pip setuptools
pip install -e . pytest pytest-cov
```

`pip install -e .` installs both `boto3` and `cast_from_env` from `setup.py`'s
`install_requires`.

## Run the tests

```bash
coverage run -m pytest && coverage report --fail-under=100
```

To run tests without the coverage gate:

```bash
python -m pytest
```

## What does NOT work

- `python setup.py test` — `setup.py` has no `test` command configured; this
  fails with `error: invalid command 'test'`. Use pytest (or `python -m unittest test -v`)
  instead.
- The `main.yml.example` CI workflow uses `python setup.py test` and is not
  active (it still has a TODO note). Do not rely on it.
