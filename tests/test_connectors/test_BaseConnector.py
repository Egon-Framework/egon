"""Tests for the ``BaseConnector`` class"""

from unittest import TestCase

from egon import Node
from egon.connectors import BaseConnector


class TestNode(Node):
    """Dummy node object for running tests"""

    def action(self):
        """Implements method required by abstract parent class"""


class NameAssignment(TestCase):
    """Test the dynamic generation of connector names"""

    def test_defaults_to_id(self) -> None:
        """Test the default connector name matches the memory ID"""

        connector = BaseConnector()
        self.assertEqual(connector.id, connector.name)

    def test_custom_name(self) -> None:
        """Test custom names are assigned to the ``name`` attribute"""

        connector = BaseConnector(name='test_name')
        self.assertEqual('test_name', connector.name)


class IDAssignment(TestCase):
    """Test the generation of instance ID values"""

    def test_is_uuid_format(self) -> None:
        """Test the instance ID is in UUID4 format"""

        self.assertRegex(TestNode().id, r'\w{8}-\w{4}-\w{4}-\w{4}-\w{12}')


class ParentNode(TestCase):
    """Test the ``parent_node`` attribute"""

    def test_parent_node_returned(self) -> None:
        """Test the parent node is returned by the ``parent_node`` attribute"""

        node = TestNode()
        connector = BaseConnector(parent_node=node)
        self.assertIs(node, connector.parent_node)


class StringRepresentation(TestCase):
    """Test the representation of connector instances as strings"""

    def test_string_representation(self) -> None:
        """Test connectors include name and ID info when cast to a string"""

        connector_name = 'my_connector'
        connector = BaseConnector(name=connector_name)

        expected_string = f'<BaseConnector(name={connector_name}) object at {connector.id}>'
        self.assertEqual(expected_string, str(connector))

    def test_repr_matches_string(self) -> None:
        """Test the ``str`` and ``repr`` strings are the same"""

        connector = BaseConnector()
        self.assertEqual(repr(connector), str(connector))
