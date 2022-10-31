"""Tests for the ``InputConnector`` class"""

from queue import Empty
from unittest import TestCase, skip

from egon.connectors import InputConnector
from egon.exceptions import MissingConnectionError


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


# TODO: Test behavior in relation to parent nodes
class Get(TestCase):
    """Test data retrieval from ``InputConnector`` instances via the ``get`` method"""

    def test_error_on_zero_refresh(self) -> None:
        """Test a ``ValueError`` is raised when ``refresh_interval`` is zero"""

        with self.assertRaises(ValueError):
            InputConnector().get(timeout=15, refresh_interval=0)

    def test_error_on_negative_refresh(self) -> None:
        """Test a ``ValueError`` is raised when ``refresh_interval`` is negative"""

        with self.assertRaises(ValueError):
            InputConnector().get(timeout=15, refresh_interval=-1)

    def test_returns_queue_value(self) -> None:
        """Test data is returned the connector queue"""

        test_val = 'test_val'
        connector = InputConnector()
        connector.put(test_val)
        self.assertEqual(test_val, connector.get(timeout=1000))

    def test_timeout_error(self) -> None:
        """Test a ``TimeoutError`` is raised when fetching an item times out"""

        connector = InputConnector()
        connector.put(1)
        with self.assertRaises(TimeoutError):
            connector.get(timeout=0)

    def test_empty_error(self):
        """Test an ``Empty`` error is raised when fetching from an empty connector"""

        with self.assertRaises(Empty):
            InputConnector().get()


class IterGet(TestCase):
    """Test data retrieval from ``InputConnector`` instances via the ``iter_get`` method"""

    @skip('Testing iter_get in this way requires finishing the node classes')
    def test_returns_queue_values(self) -> None:
        """Test values are returned from the instance queue"""

        connector = InputConnector()
        connector.put(1)
        connector.put(2)

        self.assertSequenceEqual([1, 2], list(connector.iter_get()))

    def test_missing_connection_error(self) -> None:
        """Test a ``MissingConnectionError`` error is raised if there is no parent node"""

        with self.assertRaises(MissingConnectionError):
            next(InputConnector().iter_get())

    @skip('Testing iter_get in this way requires finishing the node classes')
    def test_empty_iterable(self) -> None:
        """Test the ``iter_get`` method can be used on an empty connector instance"""

        connector = InputConnector()
        for _ in connector.iter_get():
            self.fail('No data should have been returned')
