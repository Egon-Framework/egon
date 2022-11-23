from typing import Tuple, Type

from .nodes import Node


class Pipeline:
    """Base class for orchestrating an interconnected collection of analysis nodes"""

    def __init__(self) -> None:
        """Instantiate the parent pipeline"""

        self._nodes = []

    def create_node(self, node_class: Type[Node], /, *args, **kwargs) -> Node:
        """Create a new analysis node and attach it to the current pipeline

        Returns:
            A new instance of the given node type
        """

        node = node_class(**kwargs)
        self._nodes.append(node)
        return node

    def validate(self) -> None:
        """Inspect the pipeline to ensure all nodes are properly interconnected.

        Raises:
            PipelineValidationError: For an invalid pipeline instance
        """

    def get_all_nodes(self) -> Tuple[Node, ...]:
        """Return all nodes assigned to the parent pipeline"""

        return tuple(self._nodes)

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
