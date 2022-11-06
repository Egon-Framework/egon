"""``Node`` objects represent individual analysis steps in a data analysis
pipeline.
"""

from __future__ import annotations

import abc
from typing import Iterable, Tuple

from .connectors import InputConnector, OutputConnector
from .exceptions import NodeValidationError
from .multiprocessing import MultiprocessingEngine


class Node(abc.ABC):
    """Abstract base class for constructing analysis nodes"""

    def __init__(self, name: str = None, num_processes: int = 0) -> None:
        """Instantiate a new pipeline node

        Args:
            name: Set a descriptive name for the connector object
            num_processes: The number of processes to allocate to the node instance
        """

        self.name = name or self.__class__.__name__
        self._engine = MultiprocessingEngine(num_processes)

    def _iter_attrs_by_type(self, attr_type) -> Iterable:
        """Return an iterable over instance attributes matching the given type

        All private and class methods/attributes are ignored.

        Args:
            attr_type: The object type to include in the iterator

        Returns:
            An iterable over attributes of the given type
        """

        class_attributes = dir(self.__class__)
        for attr_name, attr_value in self.__dict__.values():
            if (
                not attr_name.startswith('_') and  # Skip private attributes/methods
                attr_name not in class_attributes and  # Skip class attributes/methods
                isinstance(attr_value, attr_type)  # Only yield the correct type
            ):
                yield attr_value

    def get_processes_count(self) -> int:
        """Return number of processes assigned to the analysis node"""

        return self._engine.get_processes_count()

    def set_processes_count(self, val) -> None:
        """Update the number of processes assigned to the analysis node"""

        self._engine.set_processes_count(val)

    def input_connectors(self) -> Tuple[InputConnector, ...]:
        """Return a collection of input connectors attached to this node"""

        return tuple(self._iter_attrs_by_type(InputConnector))

    def output_connectors(self) -> Tuple[OutputConnector, ...]:
        """Return a collection of output connectors attached to this node"""

        return tuple(self._iter_attrs_by_type(OutputConnector))

    @property
    def upstream_nodes(self) -> Tuple[Node, ...]:
        """Return a list of upstream nodes connected to the current node"""

        input_connectors = self._iter_attrs_by_type(InputConnector)
        return tuple(connector.parent_node for connector in input_connectors)

    @property
    def downstream_nodes(self) -> Tuple[Node, ...]:
        """Return a list of downstream nodes connected to the current node"""

        output_connectors = self._iter_attrs_by_type(OutputConnector)
        return tuple(connector.parent_node for connector in output_connectors)

    def validate(self) -> None:
        """Validate the current node has no obvious connection issues

        Raises:
            NodeValidationError: If the node does not validate properly
        """

        for connector in self._iter_attrs_by_type(InputConnector):
            if not connector.is_connected():
                raise NodeValidationError(f'Node has an unconnected input named {connector.name}')

        for connector in self._iter_attrs_by_type(OutputConnector):
            if not connector.is_connected():
                raise NodeValidationError(f'Node has an unconnected output named {connector.name}')

    @classmethod
    def class_setup(cls) -> None:
        """Setup tasks for configuring the parent class

        This method is called once to set up the parent class before launching
        any child processes.

        For the corresponding teardown logic, see the ``class_teardown`` method.
        """

    def setup(self) -> None:
        """Setup tasks for configuring individual child processes

        This method is called once by every node instance within each
        child process.

        For the corresponding teardown logic, see the ``teardown`` method.
        """

    @abc.abstractmethod
    def action(self) -> None:
        """The primary analysis task performed by this node"""

    def teardown(self) -> None:
        """teardown tasks for cleaning up after individual child processes

        This method is called once before exiting each child process.

        For the corresponding setup logic, see the ``setup`` method.
        """

    @classmethod
    def class_teardown(cls) -> None:
        """teardown tasks for cleaning up after the parent class

        This method is called once after all child processes have been terminated.

        For the corresponding setup logic, see the ``class_setup`` method.
        """

    def _execute_helper(self) -> None:
        """Helper method for the public ``execute`` method

        This method is a wrapper for running the ``setup``, ``action``, and
        ``teardown`` methods in order.
        """

        self.setup()
        self.action()
        self.teardown()

    def execute(self) -> None:
        """Execute the pipeline node, including all setup and teardown tasks"""

        self.class_setup()
        self._engine.launch(self._execute_helper)
        self.class_teardown()

    def is_finished(self) -> bool:
        """Return whether all node processes have finished processing data"""

        return self._engine.is_finished()

    def is_expecting_data(self) -> bool:
        """Return whether the node is still expecting data from upstream nodes

        This method checks whether any connected, upstream nodes are still
        running and whether there is still data pending in any input connectors.
        """

        # Check for running nodes immediately upstream
        # These nodes may still be populating the queue
        for input_connector in self.input_connectors():
            for output_connector in input_connector.partners:
                if not output_connector.parent_node.is_finished():
                    return True

        # Check data still pending in queue
        for input_connector in self.input_connectors():
            if not input_connector.empty():
                return True

        return False

    def __str__(self) -> str:
        """Return a string representation of the parent instance"""

        return f'<{self.__class__.__name__}(name={self.name}) object at {self._id}>'
