"""Tests for the ``InputConnector`` class"""

from queue import Empty
from unittest import TestCase

from egon.connectors import InputConnector
from egon.exceptions import MissingConnectionError
from egon.nodes import Node


class DummyUpstreamNode(Node):
    """Dummy node with a single output for running tests"""

    def __init__(self):
        super().__init__(1)
        self.output = self.create_output()

    def action(self):
        """Implements method required by abstract parent class"""


class DummyDownstreamNode(Node):
    """Dummy node with a single input for running tests"""

    def __init__(self):
        super().__init__(1)
        self.input = self.create_input()

    def action(self):
        """Implements method required by abstract parent class"""


class MaxSizeValidation(TestCase):
    """Test errors are raised for invalid ``maxsize`` arguments at init"""

    def test_negative_error(self) -> None:
        with self.assertRaises(ValueError):
            InputConnector(maxsize=-1)

        with self.assertRaises(ValueError):
            InputConnector(maxsize=-1.4)

    def test_float_error(self) -> None:
        with self.assertRaises(ValueError):
            InputConnector(maxsize=1.3)

    def test_zero_allowed(self) -> None:
        connector = InputConnector(maxsize=0)
        self.assertEqual(0, connector.maxsize)


class QueueProperties(TestCase):
    """Test queue properties are properly exposed by the ``InputConnector`` class"""

    def test_size_matches_queue_size(self) -> None:
        """Test the ``size`` method returns the size of the queue`"""

        connector = InputConnector(maxsize=1)
        self.assertEqual(connector.size(), 0)

        connector._put(1)
        self.assertEqual(connector.size(), 1)

        connector.get(1)
        self.assertEqual(connector.size(), 0)

    def test_full_state(self) -> None:
        """Test the ``full`` method returns whether the queue is full"""

        connector = InputConnector(maxsize=1)
        self.assertFalse(connector.full())

        connector._put(1)
        self.assertTrue(connector.full())

        connector.get(1)
        self.assertFalse(connector.full())

    def test_empty_state(self) -> None:
        """Test the ``empty`` method returns whether the queue is empty"""

        connector = InputConnector(maxsize=1)
        self.assertTrue(connector.empty())

        connector._put(1)
        self.assertFalse(connector.empty())

        connector.get(1)
        self.assertTrue(connector.empty())

    def test_max_size(self) -> None:
        """Test the ``maxsize`` attribute returns the maximum queue size"""

        connector = InputConnector(maxsize=1)
        self.assertEqual(1, connector.maxsize)


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
        """Test data is returned from the connector queue"""

        test_val = 'test_val'
        connector = InputConnector()
        connector._put(test_val)
        self.assertEqual(test_val, connector.get(timeout=1000))

    def test_timeout_error(self) -> None:
        """Test a ``TimeoutError`` is raised when the method times out"""

        connector = InputConnector()
        connector._put(1)
        with self.assertRaises(TimeoutError):
            connector.get(timeout=0)

    def test_empty_error(self) -> None:
        """Test an ``Empty`` error is raised when fetching from an empty connector"""

        with self.assertRaises(Empty):
            InputConnector().get()


class IterGet(TestCase):
    """Test data retrieval from ``InputConnector`` instances via the ``iter_get`` method"""

    def setUp(self) -> None:
        """Create and connect two nodes"""

        self.upstream = DummyUpstreamNode()
        self.downstream = DummyDownstreamNode()
        self.upstream.output.connect(self.downstream.input)

    def test_returns_queue_values(self) -> None:
        """Test values are returned from the instance queue"""

        self.upstream.output.put(1)
        self.upstream.output.put(2)
        self.upstream.execute()

        self.assertSequenceEqual([1, 2], list(self.downstream.input.iter_get()))

    def test_missing_connection_error(self) -> None:
        """Test a ``MissingConnectionError`` error is raised if the connector has no parent node"""

        with self.assertRaises(MissingConnectionError):
            next(InputConnector().iter_get())

    def test_empty_iterable(self) -> None:
        """Test the ``iter_get`` method can be used on an empty connector instance"""

        self.upstream.execute()
        for _ in self.downstream.input.iter_get():
            self.fail('No data should have been returned')
