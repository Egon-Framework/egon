"""Pipeline objects are used to orchestrate the
execution of multiple analysis nodes.
"""

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

        Args:
            node_class: The Node subclass to instantiate
            *args: Any positional arguments for instantiating the node
            **kwargs: Any keyword arguments for instantiating the node

        Returns:
            A new instance of the given node type
        """

        node = node_class(*args, **kwargs)
        self._nodes.append(node)
        return node

    def validate(self) -> None:
        """Inspect the pipeline to ensure all nodes are properly interconnected

        This method checks for:
         - Nodes that are connected in a way that forms an infinite loop
         - Nodes that are not connected to the rest of the pipeline

        Raises:
            PipelineValidationError: For an invalid pipeline instance
        """

        if self._is_cyclic():
            raise PipelineValidationError('The analysis pipeline has a cyclical connection')

        if self._isolated_nodes():
            raise PipelineValidationError('The analysis pipeline disconnected nodes')

    def _is_cyclic_helper(self, node: Node, visited: Dict[Node, bool], recursive_stack: Dict[Node, bool]) -> bool:
        """Recursive helper function for determining if a graph is cyclic

        Args:
            node: The current node being examined
            visited: Dictionary for tracking which nodes have been visited
            recursive_stack: Dictionary for tracking which nodes have been visited this recursion cycle

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

    def _isolated_nodes_helper(self, node: Node, recursive_stack: Dict[Node, bool]) -> None:
        """Helper function for a traditional depth first search

        Args:
            node: The current node being visited
            recursive_stack: Dictionary used for tracking which nodes have been visited
        """

        # Avoid checking graph branches multiple times by exiting if already been visited
        if recursive_stack[node]:
            return

        recursive_stack[node] = True
        for neighbor in node.downstream_nodes():
            self._isolated_nodes_helper(neighbor, recursive_stack)

        for neighbor in node.upstream_nodes():
            self._isolated_nodes_helper(neighbor, recursive_stack)

    def _isolated_nodes(self) -> bool:
        """Return whether the pipeline has any isolated nodes"""

        recursive_stack = {node: False for node in self._nodes}
        self._isolated_nodes_helper(self._nodes[0], recursive_stack)
        return not all(recursive_stack.values())

    def get_all_nodes(self) -> Tuple[Node, ...]:
        """Return all nodes assigned to the parent pipeline"""

        return tuple(self._nodes)

    def is_finished(self) -> bool:
        """Return whether the pipeline is finished"""

        return all(node.is_finished() for node in self._nodes)

    def run(self) -> None:
        """Run the pipeline and wait for it to exit before continuing execution"""

        self.run_async()
        self.join()

    def run_async(self) -> None:
        """Run the pipeline asynchronously"""

        self.validate()
        for node in self.get_all_nodes():
            node._engine.run_async()

    def join(self) -> None:
        """Wait for the pipeline to exit before continuing execution"""

        for node in self.get_all_nodes():
            node._engine.join()

    def kill(self) -> None:
        """Kill all child processes assigned to the pipeline"""

        for node in self.get_all_nodes():
            node._engine.kill()
