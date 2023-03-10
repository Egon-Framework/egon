"""Tests for the ``Pipeline`` class"""

from time import sleep
from unittest import TestCase

from egon.exceptions import PipelineValidationError
from tests.utils import create_valid_pipeline, create_cyclic_pipeline, create_disconnected_pipeline


class IDAssignment(TestCase):
    """Test the generation of instance ID values"""

    def test_uuid4_format(self) -> None:
        """Test the instance ID is in UUID4 format"""

        self.assertRegex(create_valid_pipeline().id, r'\w{8}-\w{4}-\w{4}-\w{4}-\w{12}')


class Validation(TestCase):
    """Tests the ``validation`` method"""

    @staticmethod
    def test_valid_pipeline_no_error() -> None:
        """Test a valid pipeline raises no errors"""

        create_valid_pipeline().validate()

    def test_cyclic_error(self) -> None:
        """Test an error is raised for a cyclic pipeline"""

        with self.assertRaisesRegex(PipelineValidationError, 'cyclic'):
            create_cyclic_pipeline().validate()

    def test_disconnected_error(self) -> None:
        """Test an error is raised for a disconnected pipeline"""

        with self.assertRaisesRegex(PipelineValidationError, 'disconnected nodes'):
            create_disconnected_pipeline().validate()


class IsFinished(TestCase):
    """Test the ``is_finished`` method"""

    def test_false_before_run(self) -> None:
        """Test the return value is ``False`` for new instances"""

        self.assertFalse(create_valid_pipeline().is_finished())

    def test_false_while_running(self) -> None:
        """Test the return value is ``False`` while the pipeline is running"""

        pipeline = create_valid_pipeline()
        pipeline.run_async()
        self.assertFalse(pipeline.is_finished())
        pipeline.kill()

    def test_true_after_run(self) -> None:
        """Test the return value is ``True`` for executed instances"""

        pipeline = create_valid_pipeline()
        pipeline.run()
        self.assertTrue(pipeline.is_finished())


class RunAsync(TestCase):
    """Test the ``run_async`` method"""

    def test_validates_before_run(self) -> None:
        """Test invalid pipelines are prevented from running"""

        cyclic_pipeline = create_cyclic_pipeline()
        with self.assertRaisesRegex(PipelineValidationError, 'cyclic'):
            cyclic_pipeline.run_async()

        disconnected_pipeline = create_disconnected_pipeline()
        with self.assertRaisesRegex(PipelineValidationError, 'disconnected nodes'):
            disconnected_pipeline.run_async()

    def test_not_finished_while_running(self) -> None:
        """Test the pipeline is NOT marked as ``finished`` while it is still executing"""

        pipeline = create_valid_pipeline()
        pipeline.run_async()
        self.assertFalse(pipeline.is_finished())
        sleep(6)  # Let any child processes finish running

    def test_finished_after_running(self) -> None:
        """Test the pipeline is marked as ``finished`` after it finishes executing"""

        pipeline = create_valid_pipeline()
        pipeline.run_async()
        sleep(6)  # Let any child processes finish running
        self.assertTrue(pipeline.is_finished())


class Run(TestCase):
    """Test the ``run`` method"""

    def test_validates_before_run(self) -> None:
        """Test the pipeline is validated before running"""

        cyclic_pipeline = create_cyclic_pipeline()
        with self.assertRaisesRegex(PipelineValidationError, 'cyclic'):
            cyclic_pipeline.run()

        disconnected_pipeline = create_disconnected_pipeline()
        with self.assertRaisesRegex(PipelineValidationError, 'disconnected nodes'):
            disconnected_pipeline.run()

    def test_finished_after_running(self) -> None:
        """Test the pipeline is marked as ``finished`` after it finishes executing"""

        pipeline = create_valid_pipeline()
        pipeline.run()
        self.assertTrue(pipeline.is_finished())


class Kill(TestCase):
    """Test the termination of processes via the ``kill`` method"""

    def test_marked_as_finished(self) -> None:
        """Test the pipeline is marked as ``finished`` after killing processes"""

        pipeline = create_valid_pipeline()
        pipeline.run_async()
        pipeline.kill()
        self.assertTrue(pipeline.is_finished())
