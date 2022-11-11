from egon.connectors import InputConnector, OutputConnector
from egon.nodes import Node


class TestNode(Node):
    """A dummy pipeline node to use when running tests"""

    def __init__(self, num_processes: int, name: str = None) -> None:
        """Instantiate a dummy node object with two inputs and a single output"""

        super().__init__(num_processes, name)
        self.input1 = InputConnector(self, name='input1')
        self.input2 = InputConnector(self, name='input2')
        self.output = OutputConnector(self, name='output')

    def action(self) -> None:
        """Implements required method for abstract parent class"""
