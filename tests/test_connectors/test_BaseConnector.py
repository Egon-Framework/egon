"""Tests for the ``BaseConnector`` class"""

from unittest import TestCase

from egon.connectors import BaseConnector


class NameAssignment(TestCase):
    """Test the dynamic generation of connector names"""

    def test_defaults_to_id(self) -> None:
        """Test the default connector name matches the memory ID"""

        connector = BaseConnector()
        self.assertEqual(hex(id(connector)), connector.name)

    def test_custom_name(self) -> None:
        """Test custom names are assigned to the ``name`` attribute"""

        connector = BaseConnector(name='test_name')
        self.assertEqual('test_name', connector.name)


class StringRepresentation(TestCase):
    """Test the representation of connector instances as strings"""

    def test_string_representation(self) -> None:
        """Test connectors include name and ID info when cast to a string"""

        connector_name = 'my_connector'
        connector = BaseConnector(connector_name)

        expected_string = f'<BaseConnector(name={connector_name}) object at {hex(id(connector))}>'
        self.assertEqual(expected_string, str(connector))
