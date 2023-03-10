"""Helper utilities for dynamically building testing constructs."""

from egon import Node, Pipeline


class DummyNode(Node):
    """A node object that does nothing"""

    def action(self) -> None:
        """Do nothing"""


def create_valid_pipeline() -> Pipeline:
    """Return a valid pipeline with two connected nodes

    Returns:
        A valid pipeline with two nodes
    """

    pipe = Pipeline()
    pipe.d1 = pipe.create_node(DummyNode, name='d1')
    pipe.d1.out = pipe.d1.create_output()

    pipe.d2 = pipe.create_node(DummyNode, name='d2')
    pipe.d2.inp = pipe.d2.create_input()

    pipe.d1.out.connect(pipe.d2.inp)
    return pipe


def create_cyclic_pipeline() -> Pipeline:
    """Return a cyclic pipeline with two cyclically interconnected nodes

    The returned pipeline includes two nodes each with an output connected
    to the other node's input.

    Returns:
        An invalid, cyclical pipeline with two nodes
    """

    pipe = create_valid_pipeline()
    pipe.d1.inp = pipe.d1.create_input()
    pipe.d2.out = pipe.d2.create_output()
    pipe.d2.out.connect(pipe.d1.inp)
    return pipe


def create_disconnected_pipeline() -> Pipeline:
    """Return a pipeline with a disconnected node

    The returned pipeline includes two connected nodes plus a third
    disconnected node.

    Returns:
        An invalid pipeline with a third disconnected node
    """

    pipe = create_valid_pipeline()

    # Create an additional set of nodes that are connected amongst themselves
    # but not to the rest of the pipeline
    pipe.d3 = pipe.create_node(DummyNode, name='d3')
    pipe.d4 = pipe.create_node(DummyNode, name='d4')
    pipe.d3.create_output().connect(pipe.d4.create_input())

    return pipe
