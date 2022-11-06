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


class ProcessAllocation(TestCase):
    """Test the allocation of processes at class instantiation"""

    def test_correct_process_count(self) -> None:
        """Test the process count matches the value passed at init"""

        node = TestNode(num_processes=4)
        self.assertEqual(4, node.get_num_processes())

    def test_negative_processes_error(self) -> None:
        """Test a ``ValueError`` is raised for negative processes"""

        with self.assertRaises(ValueError):
            TestNode(num_processes=-1)

    def test_zero_processes_error(self) -> None:
        """Test a ``ValueError`` is raised for zero processes"""

        with self.assertRaises(ValueError):
            TestNode(num_processes=0)


class SetNumProcesses(TestCase):
    """Test setting/getting the number of node processes"""

    def test_processes_count(self) -> None:
        """Test the getter values are updated by the setter"""

        node = TestNode(num_processes=4)
        self.assertEqual(4, node.get_num_processes())

        # Increase the number of processes
        node.set_num_processes(6)
        self.assertEqual(6, node.get_num_processes())

        # Decrease the number of processes
        node.set_num_processes(2)
        self.assertEqual(2, node.get_num_processes())

    def test_negative_processes_error(self) -> None:
        """Test a ``ValueError`` is raised for negative processes"""

        node = TestNode(num_processes=4)
        with self.assertRaises(ValueError):
            node.set_num_processes(-1)

    def test_zero_processes_error(self) -> None:
        """Test a ``ValueError`` is raised for zero processes"""

        node = TestNode(num_processes=4)
        with self.assertRaises(ValueError):
            node.set_num_processes(0)


class InputConnectors(TestCase):
    """Test the ``input_connectors`` method"""

    def test_return_includes_connectors(self) -> None:
        """Test node inputs are returned as a tuple"""

        node = TestNode(num_processes=1)
        self.assertEqual((node.input1, node.input2), node.input_connectors())


class OutputConnectors(TestCase):
    """Test the ``output_connectors`` method"""

    def test_return_includes_connectors(self) -> None:
        """Test node outputs are returned as a tuple"""

        node = TestNode(num_processes=1)
        self.assertEqual((node.output,), node.output_connectors())


class StringRepresentation(TestCase):
    """Test the representation of node instances as strings"""

    def test_string_representation(self) -> None:
        """Test nodes include name and ID info when cast to a string"""

        node_name = 'my_node'
        node = TestNode(num_processes=1, name=node_name)

        expected_string = f'<TestNode(name={node_name}) object at {hex(id(node))}>'
        self.assertEqual(expected_string, str(node))
