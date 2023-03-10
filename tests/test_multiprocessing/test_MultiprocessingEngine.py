"""Tests for the ``multiprocessing`` module"""

from multiprocessing import Manager, current_process
from time import sleep
from unittest import TestCase

from egon.multiprocessing import MultiprocessingEngine


class ProcessAllocation(TestCase):
    """Test the allocation of processes at class instantiation"""

    def test_correct_process_count(self) -> None:
        """Test the process count matches the value passed at init"""

        engine = MultiprocessingEngine(num_processes=4, target=lambda: None)
        self.assertEqual(4, engine.get_num_processes())

    def test_negative_processes_error(self) -> None:
        """Test a ``ValueError`` is raised for negative processes"""

        with self.assertRaises(ValueError):
            MultiprocessingEngine(num_processes=-1, target=lambda: None)

    def test_zero_processes_error(self) -> None:
        """Test a ``ValueError`` is raised for zero processes"""

        with self.assertRaises(ValueError):
            MultiprocessingEngine(num_processes=0, target=lambda: None)


class Reset(TestCase):
    """Test the ``reset`` method"""

    def test_num_processes_settable(self) -> None:
        """Test the number of processes becomes settable after execution"""

        engine = MultiprocessingEngine(num_processes=4, target=lambda: None)
        engine.run()
        engine.reset()

        engine.set_num_processes(2)
        self.assertEqual(2, engine.get_num_processes())

    def test_not_executed_error(self) -> None:
        """Test a ``RuntimeError`` is raised when calling the method before executing the engine"""

        engine = MultiprocessingEngine(num_processes=4, target=lambda: None)
        with self.assertRaises(RuntimeError):
            engine.reset()

    @staticmethod
    def test_becomes_runnable() -> None:
        """Test executed engines become re-runnable after being reset"""

        engine = MultiprocessingEngine(num_processes=4, target=lambda: None)
        engine.run()
        engine.reset()
        engine.run()

    def test_num_processes_unchanged(self) -> None:
        """Test the number of processes does not change when and engine is reset"""

        engine = MultiprocessingEngine(num_processes=4, target=lambda: None)
        engine.run()
        engine.reset()
        self.assertEqual(4, engine.get_num_processes())


class SetNumProcesses(TestCase):
    """Test setting/getting the number of engine processes"""

    def test_processes_count(self) -> None:
        """Test the getter values are updated by the setter"""

        engine = MultiprocessingEngine(num_processes=4, target=lambda: None)
        self.assertEqual(4, engine.get_num_processes())

        # Increase the number of processes
        engine.set_num_processes(6)
        self.assertEqual(6, engine.get_num_processes())

        # Decrease the number of processes
        engine.set_num_processes(2)
        self.assertEqual(2, engine.get_num_processes())

    def test_negative_processes_error(self) -> None:
        """Test a ``ValueError`` is raised for negative processes"""

        engine = MultiprocessingEngine(num_processes=4, target=lambda: None)
        with self.assertRaises(ValueError):
            engine.set_num_processes(-1)

    def test_zero_processes_error(self) -> None:
        """Test a ``ValueError`` is raised for zero processes"""

        engine = MultiprocessingEngine(num_processes=4, target=lambda: None)
        with self.assertRaises(ValueError):
            engine.set_num_processes(0)

    def test_locked_engine_error(self) -> None:
        """Test a ``RuntimeError`` is raised when setting processes on a locked engine"""

        engine = MultiprocessingEngine(num_processes=4, target=lambda: None)
        engine.run()

        with self.assertRaises(RuntimeError):
            engine.set_num_processes(2)


class IsFinished(TestCase):
    """Test the ``is_finished`` method"""

    def test_false_before_run(self) -> None:
        """Test the return value is ``False`` for new instances"""

        self.assertFalse(MultiprocessingEngine(num_processes=4, target=lambda: None).is_finished())

    def test_false_while_running(self) -> None:
        """Test the return value is ``False`` while the engine is running"""

        engine = MultiprocessingEngine(num_processes=4, target=lambda: sleep(30))
        engine.run_async()
        self.assertFalse(engine.is_finished())
        engine.kill()

    def test_true_after_run(self) -> None:
        """Test the return value is ``True`` for executed instances"""

        engine = MultiprocessingEngine(num_processes=4, target=lambda: None)
        engine.run()
        self.assertTrue(engine.is_finished())

    def test_false_after_reset(self) -> None:
        """Test the return value is ``False`` for reset instances"""

        engine = MultiprocessingEngine(num_processes=4, target=lambda: None)
        engine.run()
        engine.reset()
        self.assertFalse(engine.is_finished())


class RunAsync(TestCase):
    """Test the ``run_async`` method"""

    def setUp(self) -> None:
        """Create an engine instance"""

        self.engine = MultiprocessingEngine(num_processes=4, target=lambda: sleep(10))

    def tearDown(self) -> None:
        """Kill any running child processes"""

        self.engine.kill()

    def test_processes_not_settable(self) -> None:
        """Test the number of processes is locked upon launch"""

        self.engine.run_async()
        with self.assertRaises(RuntimeError):
            self.engine.set_num_processes(1)

    def test_processes_are_launched(self) -> None:
        """Test child processes are launched by the method"""

        self.engine.run_async()
        for proc in self.engine._processes:
            self.assertTrue(proc.is_alive())


class Run(TestCase):
    """Test the ``run`` method"""

    def test_target_is_called(self) -> None:
        """Test the target function is evaluated in each child process"""

        shared_list = Manager().list()
        target = lambda: shared_list.append(current_process().pid)
        engine = MultiprocessingEngine(num_processes=4, target=target)
        engine.run()

        self.assertEqual(4, len(shared_list))


class Join(TestCase):
    """Test the joining of processes via the ``join`` method"""

    def test_join_before_execution_error(self) -> None:
        """Test an error is raised when joining processes before the engine has started"""

        engine = MultiprocessingEngine(num_processes=4, target=lambda: sleep(30))
        with self.assertRaisesRegex(RuntimeError, 'can only join already running processes'):
            engine.join()

    def test_join_after_execution(self) -> None:
        """Test no errors are raised when joining processes after the engine finishes executing"""

        engine = MultiprocessingEngine(num_processes=4, target=lambda: sleep(30))
        engine.run()
        engine.join()


class Kill(TestCase):
    """Test the termination of processes via the ``kill`` method"""

    def test_marked_as_finished(self) -> None:
        """Test the engine registers as finished after killing any processes"""

        engine = MultiprocessingEngine(num_processes=4, target=lambda: sleep(30))
        engine.run_async()
        engine.kill()
        self.assertTrue(engine.is_finished())

    def test_processes_are_killed(self) -> None:
        """Test all child processes are killed"""

        engine = MultiprocessingEngine(num_processes=4, target=lambda: sleep(30))
        engine.run_async()
        engine.kill()

        sleep(2)  # Wait for processes to close out before testing them
        for proc in engine._processes:
            self.assertFalse(proc.is_alive())

    def test_kill_before_execution_error(self) -> None:
        """Test an error is raised when killing processes before the engine has started"""

        engine = MultiprocessingEngine(num_processes=4, target=lambda: sleep(30))
        with self.assertRaisesRegex(RuntimeError, 'can only kill already running processes'):
            engine.kill()

    def test_kill_after_execution(self) -> None:
        """Test no errors are raised when killing processes after the engine finishes executing"""

        engine = MultiprocessingEngine(num_processes=4, target=lambda: sleep(30))
        engine.run()
        engine.kill()
