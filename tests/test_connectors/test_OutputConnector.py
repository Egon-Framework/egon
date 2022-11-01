"""Tests for the ``OutputConnector`` class"""

from unittest import TestCase

from egon.connectors import InputConnector, OutputConnector
from egon.exceptions import MissingConnectionError


class Put(TestCase):
    """Tests for the ``put`` method"""

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
        self.assertEqual(input2.get(), test_val, 'First input connector did not return data')

    def test_error_if_unconnected(self) -> None:
        """Test a ``MissingConnectionError`` error is raised if the instance is not connected"""

        with self.assertRaises(MissingConnectionError):
            OutputConnector().put(5)


class Connect(TestCase):
    """Tests for the ``connect`` method"""

    def test_error_on_connection_to_same_type(self) -> None:
        """Test an error is raised when connecting two outputs together"""

        with self.assertRaises(ValueError):
            OutputConnector().connect(OutputConnector())

    def test_duplicate_connections(self) -> None:
        """Test an error is raised when trying to overwrite an existing connection"""

        input_conn = InputConnector()
        output_conn = OutputConnector()

        output_conn.connect(input_conn)
        output_conn.connect(input_conn)
        self.assertSequenceEqual((output_conn,), input_conn.partners)
        self.assertSequenceEqual((input_conn,), output_conn.partners)
