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
