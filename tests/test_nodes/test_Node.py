"""Tests for the ``Node`` class."""

from unittest import TestCase

from egon.connectors import InputConnector, OutputConnector
from egon.nodes import Node


class TestNode(Node):
    """A dummy pipeline node to use when running tests"""

    def __init__(self, num_processes: int, name: str = None) -> None:
        """Instantiate a dummy node object with two inputs and a single output"""

        super().__init__(num_processes, name)
        self.input1 = InputConnector(name='input1')
        self.input2 = InputConnector(name='input2')
        self.output = OutputConnector(name='output')

    def action(self) -> None:
        """Implements required method for abstract parent class"""


class NameAssignment(TestCase):
    """Test the dynamic generation of node names"""

    def test_defaults_to_className(self) -> None:
        """Test the default node name matches the class name"""

        self.assertEqual('TestNode', TestNode(num_processes=1).name)

    def test_custom_name(self) -> None:
        """Test custom names are assigned to the ``name`` attribute"""

        node = TestNode(num_processes=1, name='test_name')
        self.assertEqual('test_name', node.name)

