"""Tests for the ``Node`` class."""

from unittest import TestCase
from unittest.mock import Mock, call

from egon import Node
from egon.exceptions import NodeValidationError


class TestNode(Node):
    """Dummy node object for running tests"""

    def action(self):
        """Implements method required by abstract parent class"""


class NameAssignment(TestCase):
    """Test the generation and assignment of node names"""

    def test_defaults_to_class_name(self) -> None:
        """Test the default node name matches the class name"""

        self.assertEqual('TestNode', TestNode().name)

    def test_custom_name(self) -> None:
        """Test custom names are assigned to the ``name`` attribute"""

        node = TestNode(name='test_name')
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

        node = TestNode()
        connector = node.create_input()
        self.assertIs(node, connector.parent_node)

    def test_name_assignment(self) -> None:
        """Test the connector is created with the given name"""

        node = TestNode()
        connector = node.create_input('test-name')
        self.assertIs('test-name', connector.name)

    def test_size_assignment(self) -> None:
        """Test the connector is created with the given maximum size"""

        node = TestNode()
        connector = node.create_input(maxsize=15)
        self.assertIs(15, connector.maxsize)


class CreateOutput(TestCase):
    """Test the ``create_output`` method"""

    def test_parent_node_assignment(self) -> None:
        """Test the node is attached to the connector as a parent"""

        node = TestNode()
        connector = node.create_output()
        self.assertIs(node, connector.parent_node)

    def test_name_assignment(self) -> None:
        """Test the connector is created with the given name"""

        node = TestNode()
        connector = node.create_output('test-name')
        self.assertIs('test-name', connector.name)


class InputConnectors(TestCase):
    """Test the ``input_connectors`` method"""

    def test_returns_all_inputs(self) -> None:
        """Test node inputs are returned as a tuple"""

        node = TestNode()
        node.input1 = node.create_input()
        node.input2 = node.create_input()
        node.output = node.create_output()

        self.assertEqual((node.input1, node.input2), node.input_connectors())

    def test_no_inputs_returns_empty(self) -> None:
        """Test the return value is empty for nodes with no inputs"""

        node = TestNode()
        self.assertEqual(tuple(), node.input_connectors())


class OutputConnectors(TestCase):
    """Test the ``output_connectors`` method"""

    def test_returns_all_outputs(self) -> None:
        """Test node outputs are returned as a tuple"""

        node = TestNode()
        node.input = node.create_input()
        node.output1 = node.create_output()
        node.output2 = node.create_output()

        self.assertEqual((node.output1, node.output2), node.output_connectors())

    def test_no_outputs_returns_empty(self) -> None:
        """Test the return value is empty for nodes with no outputs"""

        node = TestNode()
        self.assertEqual(tuple(), node.output_connectors())


class UpstreamNodes(TestCase):
    """Test the ``upstream_nodes`` method"""

    def test_multiple_inputs(self) -> None:
        """Test upstream nodes are returned for multiple upstreams connected by multiple inputs"""

        # Connect two upstream nodes to a single downstream node
        upstream1 = TestNode(name='upstream1')
        upstream2 = TestNode(name='upstream2')
        downstream = TestNode(name='downstream')

        upstream1.create_output().connect(downstream.create_input())
        upstream2.create_output().connect(downstream.create_input())

        # Check both upstream nodes are returned by the downstream node
        self.assertCountEqual((upstream1, upstream2), downstream.upstream_nodes())

    def test_shared_input(self) -> None:
        """Test upstream nodes are returned for multiple upstreams connected by a shared input"""

        upstream1 = TestNode(name='upstream1')
        upstream2 = TestNode(name='upstream2')
        downstream = TestNode(name='downstream')

        downstream_input = downstream.create_input()
        upstream1.create_output().connect(downstream_input)
        upstream2.create_output().connect(downstream_input)

        self.assertCountEqual((upstream1, upstream2), downstream.upstream_nodes())

    def test_empty_for_no_upstream(self) -> None:
        """Test the return value is empty when no upstream nodes are connected"""

        self.assertEqual(tuple(), TestNode().upstream_nodes())


class DownstreamNodes(TestCase):
    """Test the ``downstream_nodes`` method"""

    def test_multiple_downstream_nodes(self) -> None:
        """Test downstream nodes are returned for multiple downstreams connected by multiple output"""

        # Connect a single upstream node to two downstream nodes
        upstream = TestNode(name='upstream')
        downstream1 = TestNode(name='downstream1')
        downstream2 = TestNode(name='downstream2')

        upstream.create_output().connect(downstream1.create_input())
        upstream.create_output().connect(downstream2.create_input())

        # Check both downstream nodes are returned by the upstream node
        self.assertCountEqual((downstream1, downstream2), upstream.downstream_nodes())

    def test_shared_output(self) -> None:
        """Test downstream nodes are returned for multiple downstreams connected by a shared output"""

        upstream = TestNode(name='upstream')
        downstream1 = TestNode(name='downstream1')
        downstream2 = TestNode(name='downstream2')

        upstream_output = upstream.create_output()
        upstream_output.connect(downstream1.create_input())
        upstream_output.connect(downstream2.create_input())

        self.assertCountEqual((downstream1, downstream2), upstream.downstream_nodes())

    def test_empty_for_no_downstream(self) -> None:
        """Test the return value is empty when no downstream nodes are connected"""

        self.assertEqual(tuple(), TestNode().downstream_nodes())


class Validate(TestCase):
    """Test the ``validate`` method"""

    def test_no_connector_error(self):
        """Test a ``NodeValidationError`` is raised for nodes without any connectors"""

        with self.assertRaisesRegex(NodeValidationError, 'Node has no input or output connectors'):
            TestNode().validate()

    def test_unconnected_input_error(self):
        """Test a ``NodeValidationError`` is raised for nodes with unconnected inputs"""

        node = TestNode()
        node.create_input()

        with self.assertRaisesRegex(NodeValidationError, 'Node has an unconnected input'):
            node.validate()

    def test_unconnected_output_error(self):
        """Test a ``NodeValidationError`` is raised for nodes with unconnected outputs"""

        node = TestNode()
        node.create_output()

        with self.assertRaisesRegex(NodeValidationError, 'Node has an unconnected output'):
            node.validate()

    @staticmethod
    def test_valid_nodes_validate() -> None:
        """Test connected nodes validate successfully"""

        node1 = TestNode()
        node2 = TestNode()
        node1.create_output().connect(node2.create_input())
        node1.validate()
        node2.validate()


class Execute(TestCase):
    """Test the ``execute`` method"""

    @staticmethod
    def test_call_order_in_child_process() -> None:
        """Test the setup/teardown call order within the child processes"""

        # The ``_execute_helper`` method is responsible setup/action/teardown tasks in the child process
        mock_parent = Mock()
        TestNode._execute_helper(mock_parent)
        mock_parent.assert_has_calls([call.setup, call.action, call.teardown])

    @staticmethod
    def test_call_order_in_parent_process() -> None:
        """Test the class_setup/class_teardown call order within the parent processes"""

        mock_parent = Mock()
        TestNode.execute(mock_parent)
        mock_parent.assert_has_calls([call.class_setup, call._engine.run, call.class_teardown])


class IsFinished(TestCase):
    """Test the ``is_finished`` method"""

    def test_false_before_execution(self) -> None:
        """Test the return value is ``False`` before execution"""

        self.assertFalse(TestNode().is_finished())

    def test_true_after_execution(self) -> None:
        """Test the return value is ``True`` after execution finishes"""

        node = TestNode()
        node.execute()
        self.assertTrue(node.is_finished())


class IsExpectingData(TestCase):
    """Tests the ``is_expecting_data`` method"""

    def setUp(self) -> None:
        """Create and connect two upstream nodes to a single downstream"""

        # Create upstream nodes each with a single output
        self.upstream1 = TestNode()
        self.upstream2 = TestNode()
        self.upstream1.output = self.upstream1.create_output()
        self.upstream2.output = self.upstream2.create_output()

        # Create a downstream node with a single input
        self.downstream = TestNode()
        self.downstream.input = self.downstream.create_input()

        # Connect the nodes together
        self.upstream1.output.connect(self.downstream.input)
        self.upstream2.output.connect(self.downstream.input)

    def test_empty_input_and_finished_upstream(self) -> None:
        """Test the node is not expecting data when:
            - The input connector is empty
            - The upstream nodes are finished running
        """

        self.upstream1.execute()
        self.upstream2.execute()
        self.assertFalse(self.downstream.is_expecting_data())

    def test_empty_parent_and_running_upstream(self) -> None:
        """Test the node is expecting data when:
            - The input connector is empty
            - The upstream nodes are still running
        """

        # Check return when all upstream nodes are still running
        self.assertTrue(self.downstream.is_expecting_data())

        # Check return when only some upstream nodes are still running
        self.upstream1.execute()
        self.assertTrue(self.downstream.is_expecting_data())

    def test_full_input_and_finished_upstream(self) -> None:
        """Test the node is expecting data when:
            - The input connector is populated
            - The upstream nodes are finished running
        """

        self.upstream1.execute()
        self.upstream2.execute()
        self.downstream.input._put(5)
        self.assertTrue(self.downstream.is_expecting_data())

    def test_full_input_and_running_upstream(self) -> None:
        """Test the node is expecting data when:
            - The input connector is populated
            - The upstream nodes are still running
        """

        # Check return when all upstream nodes are still running
        self.downstream.input._put(5)
        self.assertTrue(self.downstream.is_expecting_data())

        # Check return when only some upstream nodes are still running
        self.downstream.input._put(5)
        self.upstream1.execute()
        self.assertTrue(self.downstream.is_expecting_data())


class StringRepresentation(TestCase):
    """Test the representation of node instances as strings"""

    def test_string_representation(self) -> None:
        """Test nodes include name and ID info when cast to a string"""

        node_name = 'my_node'
        node = TestNode(name=node_name)

        expected_string = f'<TestNode(name={node_name}) object at {hex(id(node))}>'
        self.assertEqual(expected_string, str(node))

    def test_repr_matches_string(self) -> None:
        """Test the ``str`` and ``repr`` strings are the same"""

        node = TestNode(name='my_node')
        self.assertEqual(repr(node), str(node))
