"""Tests for the ``Pipeline`` class"""

from unittest import TestCase

from egon.exceptions import PipelineValidationError
from egon.nodes import Node
from egon.pipeline import Pipeline


class Dummy(Node):
    """A dummy node object for testing"""

    def action(self) -> None:
        """Implements method required by abstract parent class"""


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


class Validation(TestCase):
    """Tests the ``validation`` method"""

    @staticmethod
    def test_valid_pipeline_no_error() -> None:
        """Test a valid pipeline raises no errors"""

        valid_pipeline().validate()

    def test_cyclic_error(self) -> None:
        """Test an error is raised for a cyclic pipeline"""

        with self.assertRaisesRegex(PipelineValidationError, 'cyclic'):
            pipe = cyclic_pipeline()
            pipe.validate()
