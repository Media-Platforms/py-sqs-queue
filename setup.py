from setuptools import setup, find_packages

try:
    from pypandoc import convert
    long_description = convert('README.md', 'rst')
except(IOError, ImportError):
    long_description = open('README.md').read()


setup(
    name='sqs_queue',
    version='0.1.0',
    description='AWS SQS queue consumer/publisher',
    author='Nic Wolff',
    author_email='nwolff@hearst.com',
    license='MIT',
    long_description=long_description,
    url='http://github.com/HearstCorp/py-sqs-queue',
    packages=find_packages(),
    install_requires=['boto3']
)
