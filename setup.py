from setuptools import setup

try:
    from pypandoc import convert
    long_description = convert('README.md', 'rst', 'md')
except(IOError, ImportError):
    long_description = open('README.md').read()


setup(
    name='sqs_queue',
    version='0.1.2',
    description='AWS SQS queue consumer/publisher',
    author='Nic Wolff',
    author_email='nwolff@hearst.com',
    license='MIT',
    long_description=long_description,
    url='http://github.com/HearstCorp/py-sqs-queue',
    py_modules=['sqs_queue'],
    install_requires=['boto3']
)
