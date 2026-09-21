# Running Tests

## Test stack

- Framework: Python `unittest` (stdlib).
- All AWS calls are mocked. No localstack, no docker, no AWS credentials needed.

## One-time setup (local dev)

```bash
cd /path/to/py-sqs-queue
python3 -m venv ../sqs-venv
source ../sqs-venv/bin/activate
pip install -U pip setuptools
pip install -e .
```

`pip install -e .` installs both `boto3` and `cast_from_env` from `setup.py`'s
`install_requires`.

## Run the tests

```bash
python -m unittest test -v
```

## What does NOT work

- `python setup.py test` — `setup.py` has no `test` command configured; this
  fails with `error: invalid command 'test'`. Use `python -m unittest` instead.
- The `main.yml.example` CI workflow uses `python setup.py test` and is not
  active (it still has a TODO note). Do not rely on it.
