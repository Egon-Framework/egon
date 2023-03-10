"""``Node`` objects represent individual analysis steps in a data analysis pipeline."""

from __future__ import annotations

import abc
import uuid
from typing import Tuple

from .connectors import InputConnector, OutputConnector
from .exceptions import NodeValidationError
from .multiprocessing import MultiprocessingEngine


class Node(abc.ABC):
    """Abstract base class for constructing analysis nodes"""

    def __init__(self, num_processes: int = 1, name: str = None) -> None:
        """Instantiate a new pipeline node

        Child classes should extend this method to define node inputs and outputs.

        Args:
            num_processes: The number of processes to allocate to the node instance
            name: A descriptive name for the connector object
        """

        self.name = name or self.__class__.__name__
        self._engine = MultiprocessingEngine(num_processes, self._execute_helper)
        self._inputs = []
        self._outputs = []
        self._id = str(uuid.uuid4())

        if hasattr(self, '__annotations__'):
            self._create_dynamic_connections()

    def _create_dynamic_connections(self) -> None:
        """Dynamically create input and output connectors based on class annotations"""

        for connector_name, connector_type in self.__annotations__.items():
            if connector_type is InputConnector:
                setattr(self, connector_name, self.create_input(connector_name))

            if connector_type is OutputConnector:
                setattr(self, connector_name, self.create_output(connector_name))

    @property
    def id(self) -> str:
        """Return the universally unique identifier for the parent node"""

        return self._id

    def get_num_processes(self) -> int:
        """Return number of processes assigned to the analysis node"""

        return self._engine.get_num_processes()

    def set_num_processes(self, val) -> None:
        """Update the number of processes assigned to the analysis node"""

        self._engine.set_num_processes(val)

    def create_input(self, name: str = None, maxsize: int = 0) -> InputConnector:
        """Create a new input connector and attach it to the current node

        Args:
            name: Set a descriptive name for the connector object
            maxsize: The maximum number of items to store in the connector at once

        Returns:
            An input connector attached to this node instance
        """

        connector = InputConnector(parent_node=self, name=name, maxsize=maxsize)
        self._inputs.append(connector)
        return connector

    def create_output(self, name: str = None) -> OutputConnector:
        """Create a new output connector and attach it to the current node

        Args:
            name: Set a descriptive name for the connector object

        Returns:
            An output connector attached to this node instance
        """

        connector = OutputConnector(parent_node=self, name=name)
        self._outputs.append(connector)
        return connector

    def input_connectors(self) -> Tuple[InputConnector, ...]:
        """Return all input connectors attached to this node"""

        return tuple(self._inputs)

    def output_connectors(self) -> Tuple[OutputConnector, ...]:
        """Return all output connectors attached to this node"""

        return tuple(self._outputs)

    def upstream_nodes(self) -> Tuple[Node, ...]:
        """Return all upstream nodes connected to the current node"""

        return tuple(p.parent_node for c in self._inputs for p in c.partners)

    def downstream_nodes(self) -> Tuple[Node, ...]:
        """Return all downstream nodes connected to the current node"""

        return tuple(p.parent_node for c in self._outputs for p in c.partners)

    def validate(self) -> None:
        """Validate the current node has no obvious connection issues

        Raises:
            NodeValidationError: If the node does not validate properly
        """

        if not (self._inputs or self._outputs):
            raise NodeValidationError('Node has no input or output connectors')

        for connector in self._inputs:
            if not connector.is_connected():
                raise NodeValidationError(f'Node has an unconnected input named {connector.name}')

        for connector in self._outputs:
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
        ``teardown`` methods.
        """

        self.setup()
        self.action()
        self.teardown()

    def execute(self) -> None:
        """Execute the pipeline node, including all setup and teardown tasks"""

        self.class_setup()
        self._engine.run()
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

        # Check if data is pending in any input queues
        for input_connector in self.input_connectors():
            if not input_connector.empty():
                return True

        return False

    def __repr__(self):
        """Return a string representation of the parent instance"""

        return f'<{self.__class__.__name__}(name={self.name}) object at {hex(id(self))}>'
