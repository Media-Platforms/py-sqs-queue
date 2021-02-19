from setuptools import setup

setup(
    name='sqs_queue',
    version='0.5.0',
    description='AWS SQS queue consumer/publisher',
    author='Nic Wolff',
    author_email='nwolff@hearst.com',
    license='MIT',
    long_description=open('README.md').read(),
    long_description_content_type="text/markdown",
    url='http://github.com/HearstCorp/py-sqs-queue',
    py_modules=['sqs_queue'],
    install_requires=['boto3']
)
