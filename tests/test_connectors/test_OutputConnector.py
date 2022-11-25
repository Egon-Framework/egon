"""Tests for the ``OutputConnector`` class"""

from unittest import TestCase

from egon import InputConnector, OutputConnector
from egon.exceptions import MissingConnectionError


class Put(TestCase):
    """Test the ``put`` method"""

    def test_value_passed_to_input(self) -> None:
        """Test the ``put`` method passes data to connected ``InputConnector`` instances"""

        input1 = InputConnector()
        input2 = InputConnector()
        output = OutputConnector()
        output.connect(input1)
        output.connect(input2)

        test_val = 'test_val'
        output.put(test_val)
        self.assertEqual(input1.get(), test_val, 'First input connector did not return data')
        self.assertEqual(input2.get(), test_val, 'Second input connector did not return data')

    def test_error_if_unconnected(self) -> None:
        """Test a ``MissingConnectionError`` error is raised if the instance is not connected"""

        with self.assertRaises(MissingConnectionError):
            OutputConnector().put(5)


class Connect(TestCase):
    """Test the ``connect`` method"""

    def test_is_connected(self):
        """Test connector objects are connected by the ``connect`` method"""

        input_conn = InputConnector()
        output_conn = OutputConnector()
        output_conn.connect(input_conn)

        # Test the connectors register as being connected
        self.assertTrue(input_conn.is_connected())
        self.assertTrue(output_conn.is_connected())

        # Test the connectors recognize each other as partners
        self.assertIn(output_conn, input_conn.partners)
        self.assertIn(input_conn, output_conn.partners)

    def test_error_on_connection_to_same_type(self) -> None:
        """Test a ``ValueError`` is raised when connecting two outputs together"""

        with self.assertRaises(ValueError):
            OutputConnector().connect(OutputConnector())

    def test_duplicate_connections(self) -> None:
        """Test duplicate connections cannot be created between inputs and outputs"""

        input_conn = InputConnector()
        output_conn = OutputConnector()

        output_conn.connect(input_conn)
        output_conn.connect(input_conn)
        self.assertSequenceEqual((output_conn,), input_conn.partners)
        self.assertSequenceEqual((input_conn,), output_conn.partners)


class Disconnect(TestCase):
    """Test the ``disconnect`` method"""

    def test_both_connectors_are_disconnected(self) -> None:
        """Test both connectors are no longer listed as partners"""

        # Connect and then disconnect two connectors
        input_conn = InputConnector()
        output_conn = OutputConnector()
        output_conn.connect(input_conn)
        output_conn.disconnect(input_conn)

        # Test the connectors no longer register as being connected
        self.assertFalse(input_conn.is_connected())
        self.assertFalse(output_conn.is_connected())

        # Test the connectors are no longer recognized as partners
        self.assertNotIn(output_conn, input_conn.partners)
        self.assertNotIn(input_conn, output_conn.partners)

    def test_error_if_not_connected(self) -> None:
        """Test a ``MissingConnectionError`` error is raised when disconnecting a connector that is not connected"""

        with self.assertRaises(MissingConnectionError):
            OutputConnector().disconnect(InputConnector())
