"""Tests for the ``Pipeline`` class"""

from time import sleep
from unittest import TestCase

from egon.exceptions import PipelineValidationError
from egon.nodes import Node
from egon.pipeline import Pipeline


class Dummy(Node):
    """A dummy node object for testing"""

    def action(self) -> None:
        """Sleep for 5 seconds"""

        sleep(5)


def valid_pipeline() -> Pipeline:
    """Return a valid pipeline with two connected nodes"""

    pipe = Pipeline()
    pipe.d1 = pipe.create_node(Dummy, num_processes=1, name='d1')
    pipe.d1.out = pipe.d1.create_output()

    pipe.d2 = pipe.create_node(Dummy, num_processes=1, name='d2')
    pipe.d2.inp = pipe.d2.create_input()

    pipe.d1.out.connect(pipe.d2.inp)
    return pipe


def cyclic_pipeline() -> Pipeline:
    """Return a cyclic pipeline with two interconnected nodes"""

    pipe = valid_pipeline()
    pipe.d1.inp = pipe.d1.create_input()
    pipe.d2.out = pipe.d2.create_output()
    pipe.d2.out.connect(pipe.d1.inp)
    return pipe


def disconnected_pipeline() -> Pipeline:
    """Return a cyclic pipeline with two interconnected nodes"""

    pipe = valid_pipeline()
    pipe.d3 = pipe.create_node(Dummy, num_processes=1, name='d3')
    return pipe


class Validation(TestCase):
    """Tests the ``validation`` method"""

    @staticmethod
    def test_valid_pipeline_no_error() -> None:
        """Test a valid pipeline raises no errors"""

        valid_pipeline().validate()

    def test_cyclic_error(self) -> None:
        """Test an error is raised for a cyclic pipeline"""

        with self.assertRaisesRegex(PipelineValidationError, 'cyclic'):
            cyclic_pipeline().validate()

    def test_disconnected_error(self) -> None:
        """Test an error is raised for a disconnected pipeline"""

        with self.assertRaisesRegex(PipelineValidationError, 'disconnected nodes'):
            disconnected_pipeline().validate()


class IsFinished(TestCase):
    """Test the ``is_finished`` method"""

    def test_false_before_run(self) -> None:
        """Test the return value is ``False`` for new instances"""

        self.assertFalse(valid_pipeline().is_finished())

    def test_false_while_running(self) -> None:
        """Test the return value is ``False`` while the pipeline is running"""

        pipeline = valid_pipeline()
        pipeline.run_async()
        self.assertFalse(pipeline.is_finished())
        pipeline.kill()

    def test_true_after_run(self) -> None:
        """Test the return value is ``True`` for executed instances"""

        pipeline = valid_pipeline()
        pipeline.run()
        self.assertTrue(pipeline.is_finished())


class Kill(TestCase):
    """Test the termination of processes via the ``kill`` method"""

    def test_marked_as_finished(self) -> None:
        """Test the pipeline registers as finished after killing any processes"""

        pipeline = valid_pipeline()
        pipeline.run_async()
        pipeline.kill()
        self.assertTrue(pipeline.is_finished())
