"""Tests for the ``InputConnector`` class"""

from queue import Empty
from unittest import TestCase

from egon import InputConnector, Node
from egon.exceptions import MissingConnectionError


class DummyUpstreamNode(Node):
    """Dummy node with a single output for running tests"""

    def __init__(self):
        """Define a single output connector"""

        super().__init__(1)
        self.output = self.create_output()

    def action(self):
        """Implements method required by abstract parent class"""


class DummyDownstreamNode(Node):
    """Dummy node with a single input for running tests"""

    def __init__(self):
        """Define a single input connector"""

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

    def test_error_on_negative_timeout(self) -> None:
        """Test a ``ValueError`` is raised when ``timeout`` is negative"""

        with self.assertRaises(ValueError):
            InputConnector().get(timeout=-1)

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

    def test_timeout_with_running_parent_error(self) -> None:
        """Test an ``Empty`` error is raised when fetching from an empty connector

        This is a regression test using a connector attached to a parent node
        that has not been executed yet.
        """

        upstream = DummyUpstreamNode()
        downstream = DummyDownstreamNode()
        upstream.output.connect(downstream.input)

        # This test requires the parent node is not finished executing
        self.assertTrue(downstream.is_expecting_data())

        with self.assertRaises(TimeoutError):
            downstream.input.get(timeout=4)

    def test_empty_error(self) -> None:
        """Test an ``Empty`` error is raised when fetching from an empty connector"""

        with self.assertRaises(Empty):
            InputConnector().get()

    def test_empty_with_finished_parent_error(self) -> None:
        """Test an ``Empty`` error is raised when fetching from an empty connector

        This is a regression test using a connector attached to a parent node
        that is finished executing.
        """

        upstream = DummyUpstreamNode()
        downstream = DummyDownstreamNode()
        upstream.output.connect(downstream.input)

        # This test requires the parent node is finished executing
        upstream.execute()
        self.assertFalse(downstream.is_expecting_data())

        with self.assertRaises(Empty):
            downstream.input.get(timeout=4)


class IterGet(TestCase):
    """Test data retrieval from ``InputConnector`` instances via the ``iter_get`` method"""

    def setUp(self) -> None:
        """Create and connect two nodes"""

        self.upstream = DummyUpstreamNode()
        self.downstream = DummyDownstreamNode()
        self.upstream.output.connect(self.downstream.input)

    def test_error_on_zero_refresh(self) -> None:
        """Test a ``ValueError`` is raised when ``refresh_interval`` is zero"""

        iterator = self.downstream.input.iter_get(timeout=15, refresh_interval=0)
        with self.assertRaises(ValueError):
            next(iterator)

    def test_error_on_negative_refresh(self) -> None:
        """Test a ``ValueError`` is raised when ``refresh_interval`` is negative"""

        iterator = self.downstream.input.iter_get(timeout=15, refresh_interval=-1)
        with self.assertRaises(ValueError):
            next(iterator)

    def test_error_on_negative_timeout(self) -> None:
        """Test a ``ValueError`` is raised when ``timeout`` is negative"""

        iterator = self.downstream.input.iter_get(timeout=-1)
        with self.assertRaises(ValueError):
            next(iterator)

    def test_returns_queue_values(self) -> None:
        """Test values are returned from the instance queue"""

        self.upstream.output.put(1)
        self.upstream.output.put(2)

        # The upstream node must be finished executing or ``iter_get`` will wait
        # indefinitely for more data to be produced
        self.upstream.execute()

        self.assertSequenceEqual([1, 2], list(self.downstream.input.iter_get()))

    def test_missing_connection_error(self) -> None:
        """Test a ``MissingConnectionError`` error is raised if the connector has no parent node"""

        with self.assertRaises(MissingConnectionError):
            next(InputConnector().iter_get())

    def test_empty_iterable(self) -> None:
        """Test the ``iter_get`` method can be used on an empty connector instance"""

        self.upstream.execute()
        self.assertFalse(list(self.downstream.input.iter_get()))
