from typing import Dict, Tuple, Type

from .exceptions import PipelineValidationError
from .nodes import Node


class Pipeline:
    """Base class for orchestrating an interconnected collection of analysis nodes"""

    def __init__(self) -> None:
        """Instantiate the parent pipeline

        Child classes should extend this method to define and setup pipeline nodes.
        """

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

        if self._is_cyclic():
            raise PipelineValidationError('The analysis pipeline has a cyclical connection')

        if self._isolated_nodes():
            raise PipelineValidationError('The analysis pipeline disconnected nodes')

    def _is_cyclic_helper(self, node: Node, visited, recursive_stack: Dict[Node, bool]) -> bool:
        """Recursive helper function for determining if a graph is cyclic

        Args:
            node: The current node being visited
            recursive_stack: Dictionary used for tracking which nodes have been visited

        Returns:
            If a cycle has been discovered
        """

        # Mark the node is visited
        visited[node] = True
        recursive_stack[node] = True

        # Recursively check downstream nodes
        for downstream in node.downstream_nodes():
            if not visited[downstream]:
                if self._is_cyclic_helper(downstream, visited, recursive_stack):
                    return True

            elif recursive_stack[downstream]:
                return True

        # Undo marking the current node
        recursive_stack[node] = False
        return False

    def _is_cyclic(self) -> bool:
        """Return whether a cyclic connection exists between nodes"""

        # Track which nodes have been visited already
        visited = {node: False for node in self._nodes}
        recursive_stack = {node: False for node in self._nodes}

        # Iterate over all nodes and check if that node is part of a cycle
        for node in self._nodes:
            if not visited[node] and self._is_cyclic_helper(node, visited, recursive_stack):
                return True

        return False

    def _isolated_nodes_helper(self, node: Node, recursive_stack: Dict[Node, bool], direction) -> None:
        """Helper function for a traditional depth first search

        Args:
            node: The current node being visited
            recursive_stack: Dictionary used for tracking which nodes have been visited
            direction: Visit nodes in the downstream (``True``) or upstream (``False``) direction
        """

        recursive_stack[node] = True
        if direction:
            neighbors = node.downstream_nodes()

        else:
            neighbors = node.upstream_nodes()

        for neighbor in neighbors:
            self._isolated_nodes_helper(neighbor, recursive_stack, direction)

    def _isolated_nodes(self) -> bool:
        """Return whether the pipeline has any isolated nodes"""

        recursive_stack = {node: False for node in self._nodes}
        self._isolated_nodes_helper(self._nodes[0], recursive_stack, True)
        self._isolated_nodes_helper(self._nodes[0], recursive_stack, False)

        return not all(recursive_stack.values())

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

    def is_finished(self) -> bool:
        """Return whether the pipeline is finished """

        return all(node.is_finished() for node in self._nodes)
