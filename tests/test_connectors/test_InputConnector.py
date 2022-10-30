"""Tests for the ``InputConnector`` class"""

from unittest import TestCase

from egon.connectors import InputConnector


class QueueProperties(TestCase):
    """Test return values for queue properties by the ``Connector`` class"""

    def test_size_matches_queue_size(self) -> None:
        """Test the ``size`` method returns the size of the queue`"""

        connector = InputConnector(maxsize=1)
        self.assertEqual(connector.size(), 0)

        connector.put(1)
        self.assertEqual(connector.size(), 1)

        connector.get(1)
        self.assertEqual(connector.size(), 0)

    def test_full_state(self) -> None:
        """Test the ``full`` method returns whether the queue is full"""

        connector = InputConnector(maxsize=1)
        self.assertFalse(connector.full())

        connector._queue.put(1)
        self.assertTrue(connector.full())

        connector.get(1)
        self.assertFalse(connector.full())

    def test_empty_state(self) -> None:
        """Test the ``empty`` method returns whether the queue is empty"""

        connector = InputConnector(maxsize=1)
        self.assertTrue(connector.empty())

        connector._queue.put(1)
        self.assertFalse(connector.empty())

        connector.get(1)
        self.assertTrue(connector.empty())
