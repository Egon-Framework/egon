"""Tests for the ``Pipeline`` class"""

from time import sleep
from unittest import TestCase

from egon.exceptions import PipelineValidationError, NodeValidationError
from tests.utils import create_valid_pipeline, create_cyclic_pipeline, create_disconnected_pipeline


class IDAssignment(TestCase):
    """Test the generation of instance ID values"""

    def test_uuid4_format(self) -> None:
        """Test the instance ID is in UUID4 format"""

        self.assertRegex(create_valid_pipeline().id, r'\w{8}-\w{4}-\w{4}-\w{4}-\w{12}')


class Validation(TestCase):
    """Tests the ``validation`` method"""

    def test_valid_pipeline_no_error(self) -> None:
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

    def test_invalid_node_error(self) -> None:
        """Test nodes are validated as part of the pipeline validation process"""

        pipeline = create_valid_pipeline()
        pipeline.get_all_nodes()[0].create_output('extra_output')
        with self.assertRaisesRegex(NodeValidationError, 'unconnected output'):
            pipeline.validate()


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
        sleep(5)  # Let any child processes finish running

    def test_finished_after_running(self) -> None:
        """Test the pipeline is marked as ``finished`` after it finishes executing"""

        pipeline = create_valid_pipeline()
        pipeline.run_async()
        sleep(5)  # Let any child processes finish running
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


class Join(TestCase):
    """Test the joining of processes via the ``join`` method"""

    def test_join_before_execution_error(self) -> None:
        """Test an error is raised when joining a pipeline before it has started running"""

        pipeline = create_valid_pipeline()
        with self.assertRaisesRegex(RuntimeError, 'Can only join processes after they have been started'):
            pipeline.join()

    def test_join_after_execution(self) -> None:
        """Test no errors are raised when joining a pipeline after it is finished executing"""

        pipeline = create_valid_pipeline()
        pipeline.run()
        pipeline.join()


class Kill(TestCase):
    """Test the termination of processes via the ``kill`` method"""

    def test_marked_as_finished(self) -> None:
        """Test the pipeline is marked as ``finished`` after killing processes"""

        pipeline = create_valid_pipeline()
        pipeline.run_async()
        pipeline.kill()
        self.assertTrue(pipeline.is_finished())

    def test_kill_before_execution_error(self) -> None:
        """Test an error is raised when killing a pipeline before it has started running"""

        pipeline = create_valid_pipeline()
        with self.assertRaisesRegex(RuntimeError, 'Can only kill processes after they have been started'):
            pipeline.kill()

    def test_kill_after_execution(self) -> None:
        """Test no errors are raised when killing a pipeline after it is finished executing"""

        pipeline = create_valid_pipeline()
        pipeline.run()
        pipeline.kill()
