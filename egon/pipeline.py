from typing import Tuple, Type

from .nodes import Node


class Pipeline:
    """Base class for orchestrating an interconnected collection of analysis nodes"""

    def validate(self) -> None:
        """Inspect the pipeline to ensure all nodes are properly interconnected."""

    def _get_attrs(self, attr_type: Type) -> Node:
        """Return a list of instance attributes matching the given type

        All class attributes are ignored.

        Args:
            attr_type: The object type to search for

        Returns:
            A list of attributes with type ``attr_type``
        """

        for attr_name in dir(self):
            if (not attr_name.startswith('_')) and (attr_name not in dir(self.__class__)):
                attr = getattr(self, attr_name)
                if isinstance(attr, attr_type):
                    yield attr

    def get_all_nodes(self) -> Tuple[Node, ...]:
        """Return all nodes assigned to the parent pipeline"""

        return tuple(self._get_attrs(Node))

    def kill(self) -> None:
        """Kill all child processes assigned to the pipeline"""

        for node in self.get_all_nodes():
            node._engine.kill()

    def run(self) -> None:
        """Run the pipeline and wait for it to exit before continuing execution"""

        for node in self.get_all_nodes():
            node._engine.run()

    def run_async(self) -> None:
        """Run the pipeline asynchronously"""

        for node in self.get_all_nodes():
            node._engine.run_async()

    def join(self) -> None:
        """Wait for the pipeline to exit before continuing execution"""

        for node in self.get_all_nodes():
            node._engine.join()
