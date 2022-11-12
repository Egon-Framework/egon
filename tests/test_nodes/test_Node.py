"""Tests for the ``Node`` class."""

from unittest import TestCase

from egon.nodes import Node


# Todo:
# - Test duplicate connections do not propagate in returned nodes

class TestNode(Node):
    """Dummy node object for running tests"""

    def action(self):
        """"""


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

    def test_negative_process_error(self) -> None:
        """Test a ``ValueError`` is raised for negative processes"""

        with self.assertRaises(ValueError):
            TestNode(num_processes=-1)

    def test_zero_process_error(self) -> None:
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


class CreateInput(TestCase):
    """Test the ``create_input`` method"""

    def test_parent_node_assignment(self) -> None:
        """Test the node is attached to the connector as a parent"""

        node = TestNode(num_processes=1)
        connector = node.create_input()
        self.assertIs(node, connector.parent_node)

    def test_name_assignment(self) -> None:
        """Test the connector is created with the given name"""

        node = TestNode(num_processes=1)
        connector = node.create_input('test-name')
        self.assertIs('test-name', connector.name)

    def test_size_assignment(self) -> None:
        """Test the connector is created with the given maximum size"""

        node = TestNode(num_processes=1)
        connector = node.create_input(maxsize=15)
        self.assertIs(15, connector.maxsize)


class CreateOutput(TestCase):
    """Test the ``create_output`` method"""

    def test_parent_node_assignment(self) -> None:
        """Test the node is attached to the connector as a parent"""

        node = TestNode(num_processes=1)
        connector = node.create_output()
        self.assertIs(node, connector.parent_node)

    def test_name_assignment(self) -> None:
        """Test the connector is created with the given name"""

        node = TestNode(num_processes=1)
        connector = node.create_output('test-name')
        self.assertIs('test-name', connector.name)


class InputConnectors(TestCase):
    """Test the ``input_connectors`` method"""

    def test_returns_all_inputs(self) -> None:
        """Test node inputs are returned as a tuple"""

        node = TestNode(num_processes=1)
        node.input1 = node.create_input()
        node.input2 = node.create_input()
        node.output = node.create_output()

        self.assertEqual((node.input1, node.input2), node.input_connectors())

    def test_no_inputs_returns_empty(self) -> None:
        """Test the return value is empty for nodes with no inputs"""

        node = TestNode(num_processes=1)
        self.assertEqual(tuple(), node.input_connectors())


class OutputConnectors(TestCase):
    """Test the ``output_connectors`` method"""

    def test_returns_all_outputs(self) -> None:
        """Test node outputs are returned as a tuple"""

        node = TestNode(num_processes=1)
        node.input = node.create_input()
        node.output1 = node.create_output()
        node.output2 = node.create_output()

        self.assertEqual((node.output1, node.output2), node.output_connectors())

    def test_no_outputs_returns_empty(self) -> None:
        """Test the return value is empty for nodes with no outputs"""

        node = TestNode(num_processes=1)
        self.assertEqual(tuple(), node.output_connectors())


class UpstreamNodes(TestCase):
    """Test the ``upstream_nodes`` method"""

    def test_multiple_upstream_nodes(self) -> None:
        """Test all upstream nodes are returned"""

        # Connect two upstream nodes to a single downstream node
        upstream1 = TestNode(num_processes=1, name='upstream1')
        upstream2 = TestNode(num_processes=1, name='upstream2')
        downstream = TestNode(num_processes=1, name='downstream')

        upstream1.create_output().connect(downstream.create_input())
        upstream2.create_output().connect(downstream.create_input())

        # Check both upstream nodes are returned by the downstream node
        self.assertEqual((upstream1, upstream2), downstream.upstream_nodes())

    def test_empty_for_no_upstream(self) -> None:
        """Test the return value is empty when no upstream nodes are connected"""

        self.assertEqual(tuple(), TestNode(num_processes=1).upstream_nodes())


class DownstreamNodes(TestCase):
    """Test the ``downstream_nodes`` method"""

    def test_multiple_downstream_nodes(self) -> None:
        """Test all downstream nodes are returned"""

        # Connect a single upstream node to two downstream nodes
        upstream = TestNode(num_processes=1, name='upstream')
        downstream1 = TestNode(num_processes=1, name='downstream1')
        downstream2 = TestNode(num_processes=1, name='downstream2')

        upstream.create_output().connect(downstream1.create_input())
        upstream.create_output().connect(downstream2.create_input())

        # Check both downstream nodes are returned by the upstream node
        self.assertEqual((downstream1, downstream2), upstream.downstream_nodes())

    def test_empty_for_no_downstream(self) -> None:
        """Test the return value is empty when no downstream nodes are connected"""

        self.assertEqual(tuple(), TestNode(num_processes=1).downstream_nodes())


class StringRepresentation(TestCase):
    """Test the representation of node instances as strings"""

    def test_string_representation(self) -> None:
        """Test nodes include name and ID info when cast to a string"""

        node_name = 'my_node'
        node = TestNode(num_processes=1, name=node_name)

        expected_string = f'<TestNode(name={node_name}) object at {hex(id(node))}>'
        self.assertEqual(expected_string, str(node))

    def test_repr_matches_string(self) -> None:
        """Test nodes include name and ID info when cast to a string"""

        node = TestNode(num_processes=1, name='my_node')
        self.assertEqual(repr(node), str(node))
