import json
import logging
from datetime import datetime, timezone
from signal import SIGTERM, SIG_DFL
from unittest import TestCase
from unittest.mock import MagicMock, patch, call

from sqs_queue import Message, Queue, utc_from_timestamp

# Disable all logging output for the entire test suite
logging.disable(logging.CRITICAL)


class TestUtcFromTimestamp(TestCase):

    def test_returns_datetime_when_attribute_exists(self):
        message = MagicMock()
        message.attributes = {'SentTimestamp': '1609459200000'}  # 2021-01-01 00:00:00 UTC
        result = utc_from_timestamp(message, 'SentTimestamp')
        self.assertIsInstance(result, datetime)
        self.assertEqual(result, datetime(2021, 1, 1, 0, 0, 0, tzinfo=timezone.utc))

    def test_returns_none_when_attribute_missing(self):
        message = MagicMock()
        message.attributes = {}
        result = utc_from_timestamp(message, 'SentTimestamp')
        self.assertIsNone(result)


class TestQueueInit(TestCase):

    def test_raises_valueerror_when_no_queue_or_name(self):
        with patch('sqs_queue.signal'):
            with self.assertRaises(ValueError) as ctx:
                Queue()
        self.assertIn('queue', str(ctx.exception))

    def test_uses_provided_queue(self):
        mock_queue = MagicMock()
        with patch('sqs_queue.signal'):
            q = Queue(queue=mock_queue)
        self.assertIs(q.queue, mock_queue)

    @patch('sqs_queue.boto3')
    @patch('sqs_queue.signal')
    def test_gets_queue_by_name(self, mock_signal, mock_boto3):
        mock_sqs = MagicMock()
        mock_boto3.resource.return_value = mock_sqs
        mock_queue = MagicMock()
        mock_sqs.get_queue_by_name.return_value = mock_queue

        q = Queue(queue_name='test-queue')

        mock_boto3.resource.assert_called_once_with('sqs', endpoint_url=None)
        mock_sqs.get_queue_by_name.assert_called_once_with(QueueName='test-queue')
        self.assertIs(q.queue, mock_queue)

    @patch('sqs_queue.boto3')
    @patch('sqs_queue.signal')
    def test_gets_queue_with_endpoint_url(self, mock_signal, mock_boto3):
        mock_sqs = MagicMock()
        mock_boto3.resource.return_value = mock_sqs

        Queue(queue_name='test-queue', endpoint_url='http://localhost:4566')

        mock_boto3.resource.assert_called_once_with('sqs', endpoint_url='http://localhost:4566')

    @patch('sqs_queue.boto3')
    @patch('sqs_queue.signal')
    def test_raises_when_queue_not_found_and_create_false(self, mock_signal, mock_boto3):
        mock_sqs = MagicMock()
        mock_boto3.resource.return_value = mock_sqs
        queue_not_exist = type('QueueDoesNotExist', (Exception,), {})
        mock_sqs.meta.client.exceptions.QueueDoesNotExist = queue_not_exist
        mock_sqs.get_queue_by_name.side_effect = queue_not_exist('not found')

        with self.assertRaises(queue_not_exist):
            Queue(queue_name='missing-queue', create=False)

    @patch('sqs_queue.boto3')
    @patch('sqs_queue.signal')
    def test_creates_queue_when_not_found_and_create_true(self, mock_signal, mock_boto3):
        mock_sqs = MagicMock()
        mock_boto3.resource.return_value = mock_sqs
        queue_not_exist = type('QueueDoesNotExist', (Exception,), {})
        mock_sqs.meta.client.exceptions.QueueDoesNotExist = queue_not_exist
        mock_sqs.get_queue_by_name.side_effect = queue_not_exist('not found')
        mock_created_queue = MagicMock()
        mock_sqs.create_queue.return_value = mock_created_queue

        q = Queue(queue_name='new-queue', create=True)

        mock_sqs.create_queue.assert_called_once_with(QueueName='new-queue')
        self.assertIs(q.queue, mock_created_queue)

    @patch('sqs_queue.boto3')
    @patch('sqs_queue.signal')
    def test_passes_kwargs_to_create_queue(self, mock_signal, mock_boto3):
        mock_sqs = MagicMock()
        mock_boto3.resource.return_value = mock_sqs
        queue_not_exist = type('QueueDoesNotExist', (Exception,), {})
        mock_sqs.meta.client.exceptions.QueueDoesNotExist = queue_not_exist
        mock_sqs.get_queue_by_name.side_effect = queue_not_exist('not found')

        Queue(queue_name='new-queue', create=True, Attributes={'DelaySeconds': '5'})

        mock_sqs.create_queue.assert_called_once_with(
            QueueName='new-queue', Attributes={'DelaySeconds': '5'}
        )

    def test_registers_sigterm_handler_by_default(self):
        mock_queue = MagicMock()
        with patch('sqs_queue.signal') as mock_signal:
            Queue(queue=mock_queue)
            mock_signal.assert_called_once()
            self.assertEqual(mock_signal.call_args[0][0], SIGTERM)

    def test_does_not_register_sigterm_when_disabled(self):
        mock_queue = MagicMock()
        with patch('sqs_queue.signal') as mock_signal:
            Queue(queue=mock_queue, trap_sigterm=False)
            mock_signal.assert_not_called()

    def test_stores_configuration_parameters(self):
        mock_queue = MagicMock()
        with patch('sqs_queue.signal'):
            q = Queue(
                queue=mock_queue,
                poll_wait=10,
                poll_sleep=20,
                sns=True,
                drain=True,
                batch=False
            )
        self.assertEqual(q.poll_wait, 10)
        self.assertEqual(q.poll_sleep, 20)
        self.assertTrue(q.sns)
        self.assertTrue(q.drain)
        self.assertFalse(q.batch)

    def test_stores_bulk_queue(self):
        mock_queue = MagicMock()
        mock_bulk_queue = MagicMock()
        with patch('sqs_queue.signal'):
            q = Queue(queue=mock_queue, bulk_queue=mock_bulk_queue)
        self.assertIs(q.bulk_queue, mock_bulk_queue)

    def test_bulk_queue_defaults_to_none(self):
        mock_queue = MagicMock()
        with patch('sqs_queue.signal'):
            q = Queue(queue=mock_queue)
        self.assertIsNone(q.bulk_queue)


class TestQueueIter(TestCase):

    def test_returns_consumer_generator(self):
        mock_queue = MagicMock()
        mock_queue.receive_messages.return_value = []
        with patch('sqs_queue.signal'):
            q = Queue(queue=mock_queue, drain=True)
            q.got_sigterm = False
            iterator = iter(q)
        self.assertIs(iterator, q.consumer)


class TestQueueConsumer(TestCase):

    def test_receives_batch_of_10_when_batch_true(self):
        mock_queue = MagicMock()
        mock_queue.receive_messages.return_value = []
        with patch('sqs_queue.signal'):
            q = Queue(queue=mock_queue, batch=True, drain=True)
            q.got_sigterm = False
            list(q)
        mock_queue.receive_messages.assert_called_with(
            MaxNumberOfMessages=10,
            WaitTimeSeconds=20,
            MessageAttributeNames=['All'],
            AttributeNames=['All']
        )

    def test_receives_single_message_when_batch_false(self):
        mock_queue = MagicMock()
        mock_queue.receive_messages.return_value = []
        with patch('sqs_queue.signal'):
            q = Queue(queue=mock_queue, batch=False, drain=True)
            q.got_sigterm = False
            list(q)
        mock_queue.receive_messages.assert_called_with(
            MaxNumberOfMessages=1,
            WaitTimeSeconds=20,
            MessageAttributeNames=['All'],
            AttributeNames=['All']
        )

    def test_skips_invalid_json_message(self):
        mock_queue = MagicMock()
        mock_message = MagicMock()
        mock_message.body = 'not valid json'
        mock_message.message_id = 'msg-1'
        mock_message.attributes = {}
        mock_queue.receive_messages.side_effect = [[mock_message], []]
        with patch('sqs_queue.signal'):
            q = Queue(queue=mock_queue, drain=True)
            q.got_sigterm = False
            messages = list(q)
        self.assertEqual(messages, [])

    def test_yields_valid_json_message(self):
        mock_queue = MagicMock()
        mock_message = MagicMock()
        mock_message.body = '{"key": "value"}'
        mock_message.message_id = 'msg-1'
        mock_message.attributes = {}
        mock_queue.receive_messages.side_effect = [[mock_message], []]
        with patch('sqs_queue.signal'):
            q = Queue(queue=mock_queue, drain=True)
            q.got_sigterm = False
            consumer = iter(q)
            msg = next(consumer)
        self.assertIsInstance(msg, Message)
        self.assertEqual(msg['key'], 'value')

    def test_extracts_sns_metadata(self):
        mock_queue = MagicMock()
        mock_message = MagicMock()
        sns_body = {
            'MessageId': 'sns-msg-id',
            'SequenceNumber': '123',
            'Timestamp': '2021-01-01T00:00:00Z',
            'Message': '{"data": "test"}'
        }
        mock_message.body = json.dumps(sns_body)
        mock_message.message_id = 'msg-1'
        mock_message.attributes = {}
        mock_queue.receive_messages.side_effect = [[mock_message], []]
        with patch('sqs_queue.signal'):
            q = Queue(queue=mock_queue, sns=True, drain=True)
            q.got_sigterm = False
            consumer = iter(q)
            msg = next(consumer)
        self.assertEqual(msg['data'], 'test')
        self.assertEqual(msg['sns_message_id'], 'sns-msg-id')
        self.assertEqual(msg['sns_sequence_number'], '123')
        self.assertEqual(msg['sns_timestamp'], '2021-01-01T00:00:00Z')

    def test_skips_sns_with_invalid_message_json(self):
        mock_queue = MagicMock()
        mock_message = MagicMock()
        sns_body = {
            'MessageId': 'sns-msg-id',
            'Message': 'not valid json'
        }
        mock_message.body = json.dumps(sns_body)
        mock_message.message_id = 'msg-1'
        mock_message.attributes = {}
        mock_queue.receive_messages.side_effect = [[mock_message], []]
        with patch('sqs_queue.signal'):
            q = Queue(queue=mock_queue, sns=True, drain=True)
            q.got_sigterm = False
            messages = list(q)
        self.assertEqual(messages, [])

    def test_skips_sns_with_missing_key(self):
        mock_queue = MagicMock()
        mock_message = MagicMock()
        sns_body = {'Message': '{"data": "test"}'}  # Missing MessageId
        mock_message.body = json.dumps(sns_body)
        mock_message.message_id = 'msg-1'
        mock_message.attributes = {}
        mock_queue.receive_messages.side_effect = [[mock_message], []]
        with patch('sqs_queue.signal'):
            q = Queue(queue=mock_queue, sns=True, drain=True)
            q.got_sigterm = False
            messages = list(q)
        self.assertEqual(messages, [])

    def test_deletes_message_after_processing(self):
        mock_queue = MagicMock()
        mock_message = MagicMock()
        mock_message.body = '{"key": "value"}'
        mock_message.message_id = 'msg-1'
        mock_message.attributes = {}
        mock_queue.receive_messages.side_effect = [[mock_message], []]
        with patch('sqs_queue.signal'):
            q = Queue(queue=mock_queue, drain=True)
            q.got_sigterm = False
            consumer = iter(q)
            next(consumer)
            try:
                next(consumer)
            except StopIteration:
                pass
        mock_message.delete.assert_called_once()

    def test_handles_delete_exception(self):
        mock_queue = MagicMock()
        mock_message = MagicMock()
        mock_message.body = '{"key": "value"}'
        mock_message.message_id = 'msg-1'
        mock_message.attributes = {}
        mock_message.delete.side_effect = Exception('delete failed')
        mock_queue.receive_messages.side_effect = [[mock_message], []]
        with patch('sqs_queue.signal'):
            q = Queue(queue=mock_queue, drain=True)
            q.got_sigterm = False
            consumer = iter(q)
            next(consumer)
            # Should not raise, just log warning
            try:
                next(consumer)
            except StopIteration:
                pass

    def test_leaves_message_in_queue_when_deferred(self):
        mock_queue = MagicMock()
        mock_message = MagicMock()
        mock_message.body = '{"key": "value"}'
        mock_message.message_id = 'msg-1'
        mock_message.attributes = {}
        mock_queue.receive_messages.side_effect = [[mock_message], []]
        with patch('sqs_queue.signal'):
            q = Queue(queue=mock_queue, drain=True)
            q.got_sigterm = False
            consumer = iter(q)
            next(consumer)
            consumer.send(True)  # Signal to leave in queue
            try:
                next(consumer)
            except StopIteration:
                pass
        mock_message.delete.assert_not_called()

    def test_exits_when_drain_true_and_queue_empty(self):
        mock_queue = MagicMock()
        mock_queue.receive_messages.return_value = []
        with patch('sqs_queue.signal'):
            q = Queue(queue=mock_queue, drain=True)
            q.got_sigterm = False
            messages = list(q)
        self.assertEqual(messages, [])
        mock_queue.receive_messages.assert_called_once()

    @patch('sqs_queue.sleep')
    def test_sleeps_when_drain_false_and_queue_empty(self, mock_sleep):
        mock_queue = MagicMock()
        mock_queue.receive_messages.return_value = []
        with patch('sqs_queue.signal'):
            q = Queue(queue=mock_queue, drain=False, poll_sleep=30)
            q.got_sigterm = False
            consumer = q.queue_consumer()
            # Run one iteration, then set sigterm to exit
            mock_queue.receive_messages.side_effect = [[], []]

            def set_sigterm(*args):
                q.got_sigterm = True
            mock_sleep.side_effect = set_sigterm
            list(consumer)
        mock_sleep.assert_called_with(30)

    @patch('sqs_queue.sleep')
    def test_yields_from_bulk_queue_when_main_queue_empty(self, mock_sleep):
        mock_queue = MagicMock()
        mock_bulk_queue = MagicMock()
        mock_sqs_msg1 = MagicMock()
        mock_sqs_msg1.message_id = 'bulk-1'
        mock_sqs_msg2 = MagicMock()
        mock_sqs_msg2.message_id = 'bulk-2'
        bulk_messages = [
            Message({'bulk': 1}, mock_bulk_queue, mock_sqs_msg1),
            Message({'bulk': 2}, mock_bulk_queue, mock_sqs_msg2),
        ]
        mock_bulk_queue.receive.side_effect = [bulk_messages, []]
        with patch('sqs_queue.signal'):
            q = Queue(queue=mock_queue, bulk_queue=mock_bulk_queue, drain=False)
            q.got_sigterm = False
            # First poll returns empty, second poll after bulk processing also empty
            mock_queue.receive_messages.side_effect = [[], []]

            def set_sigterm(*args):
                q.got_sigterm = True
            mock_sleep.side_effect = set_sigterm
            messages = list(q)
        self.assertEqual(len(messages), 2)
        self.assertEqual(messages[0]['bulk'], 1)
        self.assertEqual(messages[1]['bulk'], 2)

    @patch('sqs_queue.sleep')
    def test_deletes_bulk_queue_messages_after_processing(self, mock_sleep):
        mock_queue = MagicMock()
        mock_bulk_queue = MagicMock()
        mock_sqs_msg1 = MagicMock()
        mock_sqs_msg1.message_id = 'bulk-1'
        mock_sqs_msg2 = MagicMock()
        mock_sqs_msg2.message_id = 'bulk-2'
        bulk_messages = [
            Message({'bulk': 1}, mock_bulk_queue, mock_sqs_msg1),
            Message({'bulk': 2}, mock_bulk_queue, mock_sqs_msg2),
        ]
        mock_bulk_queue.receive.side_effect = [bulk_messages, []]
        with patch('sqs_queue.signal'):
            q = Queue(queue=mock_queue, bulk_queue=mock_bulk_queue, drain=False)
            q.got_sigterm = False
            mock_queue.receive_messages.side_effect = [[], []]

            def set_sigterm(*args):
                q.got_sigterm = True
            mock_sleep.side_effect = set_sigterm
            list(q)
        mock_sqs_msg1.delete.assert_called_once()
        mock_sqs_msg2.delete.assert_called_once()

    @patch('sqs_queue.sleep')
    def test_does_not_sleep_after_yielding_bulk_messages(self, mock_sleep):
        mock_queue = MagicMock()
        mock_bulk_queue = MagicMock()
        mock_sqs_msg = MagicMock()
        mock_sqs_msg.message_id = 'bulk-1'
        bulk_messages = [Message({'bulk': 1}, mock_bulk_queue, mock_sqs_msg)]
        mock_bulk_queue.receive.side_effect = [bulk_messages, []]
        with patch('sqs_queue.signal'):
            q = Queue(queue=mock_queue, bulk_queue=mock_bulk_queue, drain=False)
            q.got_sigterm = False
            # First poll empty (triggers bulk), second poll empty (triggers sleep)
            mock_queue.receive_messages.side_effect = [[], []]

            def set_sigterm(*args):
                q.got_sigterm = True
            mock_sleep.side_effect = set_sigterm
            list(q)
        # Should have polled twice: once before bulk, once after bulk (then sleep)
        self.assertEqual(mock_queue.receive_messages.call_count, 2)
        # Sleep only called once (after second empty poll when bulk is exhausted)
        mock_sleep.assert_called_once()

    @patch('sqs_queue.sleep')
    def test_sleeps_when_bulk_queue_empty(self, mock_sleep):
        mock_queue = MagicMock()
        mock_queue.receive_messages.return_value = []
        mock_bulk_queue = MagicMock()
        mock_bulk_queue.receive.return_value = []
        with patch('sqs_queue.signal'):
            q = Queue(queue=mock_queue, bulk_queue=mock_bulk_queue, drain=False)
            q.got_sigterm = False

            def set_sigterm(*args):
                q.got_sigterm = True
            mock_sleep.side_effect = set_sigterm
            list(q)
        # Should sleep since bulk queue had no messages
        mock_sleep.assert_called_once()

    @patch('sqs_queue.sleep')
    def test_bulk_queue_receive_called_with_max_count_batch_true(self, mock_sleep):
        mock_queue = MagicMock()
        mock_queue.receive_messages.return_value = []
        mock_bulk_queue = MagicMock()
        mock_bulk_queue.receive.return_value = []
        with patch('sqs_queue.signal'):
            q = Queue(queue=mock_queue, bulk_queue=mock_bulk_queue, drain=False, batch=True)
            q.got_sigterm = False

            def set_sigterm(*args):
                q.got_sigterm = True
            mock_sleep.side_effect = set_sigterm
            list(q)
        # batch=True means max_count=10
        mock_bulk_queue.receive.assert_called_with(10)

    @patch('sqs_queue.sleep')
    def test_bulk_queue_receive_called_with_max_count_batch_false(self, mock_sleep):
        mock_queue = MagicMock()
        mock_queue.receive_messages.return_value = []
        mock_bulk_queue = MagicMock()
        mock_bulk_queue.receive.return_value = []
        with patch('sqs_queue.signal'):
            q = Queue(queue=mock_queue, bulk_queue=mock_bulk_queue, drain=False, batch=False)
            q.got_sigterm = False

            def set_sigterm(*args):
                q.got_sigterm = True
            mock_sleep.side_effect = set_sigterm
            list(q)
        # batch=False means max_count=1
        mock_bulk_queue.receive.assert_called_with(1)

    def test_drain_mode_also_drains_bulk_queue(self):
        mock_queue = MagicMock()
        mock_queue.receive_messages.return_value = []
        mock_bulk_queue = MagicMock()
        mock_sqs_msg = MagicMock()
        mock_sqs_msg.message_id = 'bulk-1'
        bulk_message = Message({'bulk': 1}, mock_bulk_queue, mock_sqs_msg)
        mock_bulk_queue.receive.side_effect = [[bulk_message], []]
        with patch('sqs_queue.signal'):
            q = Queue(queue=mock_queue, bulk_queue=mock_bulk_queue, drain=True)
            q.got_sigterm = False
            messages = list(q)
        # drain=True should also drain bulk_queue
        self.assertEqual(len(messages), 1)
        self.assertEqual(messages[0]['bulk'], 1)

    def test_bulk_queue_not_checked_when_main_queue_has_messages(self):
        mock_queue = MagicMock()
        mock_message = MagicMock()
        mock_message.body = '{"key": "value"}'
        mock_message.message_id = 'msg-1'
        mock_message.attributes = {}
        # First receive returns message, bulk_queue not checked
        mock_queue.receive_messages.return_value = [mock_message]
        mock_bulk_queue = MagicMock()
        with patch('sqs_queue.signal'):
            q = Queue(queue=mock_queue, bulk_queue=mock_bulk_queue, drain=True)
            q.got_sigterm = False
            consumer = iter(q)
            # Process first message - bulk_queue should not be checked yet
            next(consumer)
        mock_bulk_queue.receive.assert_not_called()

    def test_puts_unprocessed_messages_back_on_sigterm(self):
        mock_queue = MagicMock()
        mock_message1 = MagicMock()
        mock_message1.body = '{"key": "value1"}'
        mock_message1.message_id = 'msg-1'
        mock_message1.attributes = {}
        mock_message1.receipt_handle = 'handle-1'
        mock_message2 = MagicMock()
        mock_message2.body = '{"key": "value2"}'
        mock_message2.message_id = 'msg-2'
        mock_message2.attributes = {}
        mock_message2.receipt_handle = 'handle-2'
        with patch('sqs_queue.signal'):
            q = Queue(queue=mock_queue, drain=True)
            q.got_sigterm = False

            def set_sigterm_and_return(*args, **kwargs):
                q.got_sigterm = True
                return [mock_message1, mock_message2]
            mock_queue.receive_messages.side_effect = set_sigterm_and_return
            list(q)
        mock_queue.change_message_visibility_batch.assert_called_once()
        entries = mock_queue.change_message_visibility_batch.call_args[1]['Entries']
        self.assertEqual(len(entries), 2)
        self.assertEqual(entries[0]['ReceiptHandle'], 'handle-1')
        self.assertEqual(entries[0]['VisibilityTimeout'], 0)

    def test_sigterm_mid_processing_defers_remaining(self):
        mock_queue = MagicMock()
        mock_message1 = MagicMock()
        mock_message1.body = '{"key": "value1"}'
        mock_message1.message_id = 'msg-1'
        mock_message1.attributes = {}
        mock_message1.receipt_handle = 'handle-1'
        mock_message2 = MagicMock()
        mock_message2.body = '{"key": "value2"}'
        mock_message2.message_id = 'msg-2'
        mock_message2.attributes = {}
        mock_message2.receipt_handle = 'handle-2'
        mock_queue.receive_messages.return_value = [mock_message1, mock_message2]
        with patch('sqs_queue.signal'):
            q = Queue(queue=mock_queue, drain=True)
            q.got_sigterm = False
            consumer = iter(q)
            # Process first message
            next(consumer)
            # Set sigterm before processing second
            q.got_sigterm = True
            try:
                next(consumer)
            except StopIteration:
                pass
        # Second message should be put back
        mock_queue.change_message_visibility_batch.assert_called_once()
        entries = mock_queue.change_message_visibility_batch.call_args[1]['Entries']
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0]['ReceiptHandle'], 'handle-2')


class TestQueueReceive(TestCase):

    def test_receive_returns_messages(self):
        mock_queue = MagicMock()
        mock_sqs_message = MagicMock()
        mock_sqs_message.body = '{"key": "value"}'
        mock_sqs_message.message_id = 'msg-1'
        mock_queue.receive_messages.return_value = [mock_sqs_message]
        with patch('sqs_queue.signal'):
            q = Queue(queue=mock_queue)
            messages = q.receive()
        self.assertEqual(len(messages), 1)
        self.assertEqual(messages[0]['key'], 'value')
        self.assertIs(messages[0].sqs_message, mock_sqs_message)

    def test_receive_uses_wait_time_zero(self):
        mock_queue = MagicMock()
        mock_queue.receive_messages.return_value = []
        with patch('sqs_queue.signal'):
            q = Queue(queue=mock_queue)
            q.receive()
        mock_queue.receive_messages.assert_called_once_with(
            MaxNumberOfMessages=10,
            WaitTimeSeconds=0,
            MessageAttributeNames=['All'],
            AttributeNames=['All']
        )

    def test_receive_respects_max_count(self):
        mock_queue = MagicMock()
        mock_queue.receive_messages.return_value = []
        with patch('sqs_queue.signal'):
            q = Queue(queue=mock_queue)
            q.receive(max_count=5)
        mock_queue.receive_messages.assert_called_once_with(
            MaxNumberOfMessages=5,
            WaitTimeSeconds=0,
            MessageAttributeNames=['All'],
            AttributeNames=['All']
        )

    def test_receive_skips_invalid_json(self):
        mock_queue = MagicMock()
        valid_msg = MagicMock()
        valid_msg.body = '{"key": "value"}'
        valid_msg.message_id = 'msg-1'
        invalid_msg = MagicMock()
        invalid_msg.body = 'not json'
        invalid_msg.message_id = 'msg-2'
        mock_queue.receive_messages.return_value = [valid_msg, invalid_msg]
        with patch('sqs_queue.signal'):
            q = Queue(queue=mock_queue)
            messages = q.receive()
        self.assertEqual(len(messages), 1)
        self.assertEqual(messages[0]['key'], 'value')

    def test_receive_returns_empty_list_when_no_messages(self):
        mock_queue = MagicMock()
        mock_queue.receive_messages.return_value = []
        with patch('sqs_queue.signal'):
            q = Queue(queue=mock_queue)
            messages = q.receive()
        self.assertEqual(messages, [])

    def test_receive_uses_wait_parameter(self):
        mock_queue = MagicMock()
        mock_queue.receive_messages.return_value = []
        with patch('sqs_queue.signal'):
            q = Queue(queue=mock_queue)
            q.receive(max_count=5, wait=20)
        mock_queue.receive_messages.assert_called_once_with(
            MaxNumberOfMessages=5,
            WaitTimeSeconds=20,
            MessageAttributeNames=['All'],
            AttributeNames=['All']
        )


class TestParseJson(TestCase):

    def test_returns_parsed_body_for_valid_json(self):
        mock_queue = MagicMock()
        with patch('sqs_queue.signal'):
            q = Queue(queue=mock_queue)
        message = MagicMock()
        message.body = '{"key": "value"}'
        result = q._parse_json(message)
        self.assertEqual(result, {'key': 'value'})

    def test_returns_none_for_invalid_json(self):
        mock_queue = MagicMock()
        with patch('sqs_queue.signal'):
            q = Queue(queue=mock_queue)
        message = MagicMock()
        message.body = 'not valid json'
        message.message_id = 'msg-1'
        self.assertIsNone(q._parse_json(message))


class TestUnwrapSns(TestCase):

    def test_unwraps_sns_message_in_place(self):
        mock_queue = MagicMock()
        with patch('sqs_queue.signal'):
            q = Queue(queue=mock_queue)
        mock_sqs_message = MagicMock()
        message = Message({
            'MessageId': 'sns-msg-id',
            'SequenceNumber': '123',
            'Timestamp': '2021-01-01T00:00:00Z',
            'Message': '{"data": "test"}'
        }, q, mock_sqs_message)
        result = q._unwrap_sns(message)
        self.assertTrue(result)
        self.assertEqual(message['data'], 'test')
        self.assertEqual(message['sns_message_id'], 'sns-msg-id')
        self.assertEqual(message['sns_sequence_number'], '123')
        self.assertEqual(message['sns_timestamp'], '2021-01-01T00:00:00Z')
        self.assertNotIn('MessageId', message)
        self.assertNotIn('Message', message)

    def test_returns_false_for_invalid_inner_json(self):
        mock_queue = MagicMock()
        with patch('sqs_queue.signal'):
            q = Queue(queue=mock_queue)
        mock_sqs_message = MagicMock()
        mock_sqs_message.message_id = 'msg-1'
        message = Message({
            'MessageId': 'sns-msg-id',
            'Message': 'not valid json'
        }, q, mock_sqs_message)
        result = q._unwrap_sns(message)
        self.assertFalse(result)

    def test_returns_false_for_missing_message_id(self):
        mock_queue = MagicMock()
        with patch('sqs_queue.signal'):
            q = Queue(queue=mock_queue)
        mock_sqs_message = MagicMock()
        mock_sqs_message.message_id = 'msg-1'
        message = Message({
            'Message': '{"data": "test"}'
        }, q, mock_sqs_message)
        result = q._unwrap_sns(message)
        self.assertFalse(result)


class TestDeleteMessage(TestCase):

    def test_deletes_sqs_message(self):
        mock_queue = MagicMock()
        with patch('sqs_queue.signal'):
            q = Queue(queue=mock_queue)
        mock_sqs_message = MagicMock()
        mock_sqs_message.message_id = 'msg-1'
        message = Message({'key': 'value'}, q, mock_sqs_message)
        q._delete_message(message)
        mock_sqs_message.delete.assert_called_once()

    def test_handles_delete_exception(self):
        mock_queue = MagicMock()
        with patch('sqs_queue.signal'):
            q = Queue(queue=mock_queue)
        mock_sqs_message = MagicMock()
        mock_sqs_message.message_id = 'msg-1'
        mock_sqs_message.delete.side_effect = Exception('delete failed')
        message = Message({'key': 'value'}, q, mock_sqs_message)
        # Should not raise
        q._delete_message(message)


class TestQueuePublish(TestCase):

    def test_sends_message_to_queue(self):
        mock_queue = MagicMock()
        with patch('sqs_queue.signal'):
            q = Queue(queue=mock_queue)
            q.publish('test message')
        mock_queue.send_message.assert_called_once_with(MessageBody='test message')

    def test_passes_kwargs_to_send_message(self):
        mock_queue = MagicMock()
        with patch('sqs_queue.signal'):
            q = Queue(queue=mock_queue)
            q.publish('test', DelaySeconds=10, MessageGroupId='group1')
        mock_queue.send_message.assert_called_once_with(
            MessageBody='test',
            DelaySeconds=10,
            MessageGroupId='group1'
        )


class TestMakeSigtermHandler(TestCase):

    def test_handler_sets_got_sigterm_flag(self):
        mock_queue = MagicMock()
        with patch('sqs_queue.signal'):
            with patch('sqs_queue.getsignal', return_value=SIG_DFL):
                q = Queue(queue=mock_queue, trap_sigterm=False)
                handler = q.make_sigterm_handler()
                q.got_sigterm = False
                handler(SIGTERM, None)
        self.assertTrue(q.got_sigterm)

    def test_handler_calls_existing_handler(self):
        mock_queue = MagicMock()
        existing_handler = MagicMock()
        with patch('sqs_queue.signal'):
            with patch('sqs_queue.getsignal', return_value=existing_handler):
                q = Queue(queue=mock_queue, trap_sigterm=False)
                handler = q.make_sigterm_handler()
                mock_frame = MagicMock()
                handler(SIGTERM, mock_frame)
        existing_handler.assert_called_once_with(SIGTERM, mock_frame)

    def test_handler_ignores_non_callable_existing_handler(self):
        mock_queue = MagicMock()
        with patch('sqs_queue.signal'):
            with patch('sqs_queue.getsignal', return_value=SIG_DFL):
                q = Queue(queue=mock_queue, trap_sigterm=False)
                handler = q.make_sigterm_handler()
                # Should not raise when existing handler is not callable
                handler(SIGTERM, None)
        self.assertTrue(q.got_sigterm)


class TestMessage(TestCase):

    def test_init_stores_body_as_dict(self):
        mock_queue = MagicMock(spec=Queue)
        body = {'key': 'value', 'nested': {'a': 1}}
        msg = Message(body, mock_queue)
        self.assertEqual(msg['key'], 'value')
        self.assertEqual(msg['nested'], {'a': 1})

    def test_init_stores_queue_reference(self):
        mock_queue = MagicMock(spec=Queue)
        msg = Message({}, mock_queue)
        self.assertIs(msg.queue, mock_queue)

    def test_init_stores_sqs_message(self):
        mock_queue = MagicMock(spec=Queue)
        mock_sqs_message = MagicMock()
        msg = Message({}, mock_queue, mock_sqs_message)
        self.assertIs(msg.sqs_message, mock_sqs_message)

    def test_init_sqs_message_defaults_to_none(self):
        mock_queue = MagicMock(spec=Queue)
        msg = Message({}, mock_queue)
        self.assertIsNone(msg.sqs_message)

    def test_defer_sends_true_to_consumer(self):
        mock_queue = MagicMock(spec=Queue)
        mock_consumer = MagicMock()
        mock_queue.consumer = mock_consumer
        msg = Message({}, mock_queue)
        msg.defer()
        mock_consumer.send.assert_called_once_with(True)
