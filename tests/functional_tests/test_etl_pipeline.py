"""Test package behavior against a traditional ETL style pipeline.

This test builds and executes an ETL pipeline with three nodes:

1. The extract node fetches data from a global queue
2. The transform step passes along the data unmodified
3. The load step puts data into a global queue
"""

from multiprocessing import Queue
from queue import Empty
from unittest import TestCase

from egon import Node, Pipeline

INPUT_QUEUE = Queue()  # Queue for the pipeline to load data from
OUTPUT_QUEUE = Queue()  # Queue for the pipeline to write data to

# Populate the input queue with test values
INPUT_VALUES = list(range(10))
for i in INPUT_VALUES:
    INPUT_QUEUE.put(i)


class Extract(Node):
    """Simple implementation for a traditional ETL extract node"""

    def __init__(self, num_processes: int = 1) -> None:
        """Define a pipeline node with a single output"""

        super().__init__(num_processes=num_processes)
        self.output = self.create_output()

    def action(self) -> None:
        """Load values from the ``INPUT_QUEUE`` collection into the pipeline"""

        while True:
            try:
                self.output.put(INPUT_QUEUE.get(timeout=2))

            except Empty:
                break


class Transform(Node):
    """Simple implementation for a traditional ETL transform node"""

    def __init__(self, num_processes: int = 1) -> None:
        """Define a pipeline node with a single input and output"""

        super().__init__(num_processes=num_processes)
        self.input = self.create_input()
        self.output = self.create_output()

    def action(self) -> None:
        """Pass data along to downstream nodes without modification"""

        for val in self.input.iter_get():
            self.output.put(val)


class Load(Node):
    """Simple implementation for a traditional ETL load node"""

    def __init__(self, num_processes: int = 1) -> None:
        """Define a pipeline node with a single input"""

        super().__init__(num_processes=num_processes)
        self.input = self.create_input()

    def action(self) -> None:
        """Load pipeline values into the ``OUTPUT_QUEUE`` collection"""

        for val in self.input.iter_get():
            OUTPUT_QUEUE.put(val)


class AddingPipeline(Pipeline):
    """A pipeline for generating and then adding numbers"""

    def __init__(self) -> None:
        """Instantiate a three node, ETL style pipeline"""

        super().__init__()

        # Instantiate nodes with a variety of different process counts
        self.extract = self.create_node(Extract, num_processes=3)
        self.transform = self.create_node(Transform, num_processes=2)
        self.load = self.create_node(Load, num_processes=1)

        self.extract.output.connect(self.transform.input)
        self.transform.output.connect(self.load.input)


class TestPipelineThroughput(TestCase):
    """Test data is successfully passed through the entire pipeline"""

    def runTest(self) -> None:
        """Test all input values are present in the pipeline output"""

        AddingPipeline().run()

        output_as_list = []
        while OUTPUT_QUEUE.qsize() != 0:
            output_as_list.append(OUTPUT_QUEUE.get())

        self.assertCountEqual(INPUT_VALUES, output_as_list)
