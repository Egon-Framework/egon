"""Tests for the ``multiprocessing`` module"""

from unittest import TestCase

from egon.multiprocessing import MultiprocessingEngine


class ProcessAllocation(TestCase):
    """Test the allocation of processes at class instantiation"""

    def test_correct_process_count(self) -> None:
        """Test the process count matches the value passed at init"""

        engine = MultiprocessingEngine(num_processes=4, target=lambda: 1)
        self.assertEqual(4, engine.get_num_processes())

    def test_negative_processes_error(self) -> None:
        """Test a ``ValueError`` is raised for negative processes"""

        with self.assertRaises(ValueError):
            MultiprocessingEngine(num_processes=-1, target=lambda: 1)

    def test_zero_processes_error(self) -> None:
        """Test a ``ValueError`` is raised for zero processes"""

        with self.assertRaises(ValueError):
            MultiprocessingEngine(num_processes=0, target=lambda: 1)


class SetNumProcesses(TestCase):
    """Test setting/getting the number of engine processes"""

    def test_processes_count(self) -> None:
        """Test the getter values are updated by the setter"""

        engine = MultiprocessingEngine(num_processes=4, target=lambda: 1)
        self.assertEqual(4, engine.get_num_processes())

        # Increase the number of processes
        engine.set_num_processes(6)
        self.assertEqual(6, engine.get_num_processes())

        # Decrease the number of processes
        engine.set_num_processes(2)
        self.assertEqual(2, engine.get_num_processes())

    def test_negative_processes_error(self) -> None:
        """Test a ``ValueError`` is raised for negative processes"""

        engine = MultiprocessingEngine(num_processes=4, target=lambda: 1)
        with self.assertRaises(ValueError):
            engine.set_num_processes(-1)

    def test_zero_processes_error(self) -> None:
        """Test a ``ValueError`` is raised for zero processes"""

        engine = MultiprocessingEngine(num_processes=4, target=lambda: 1)
        with self.assertRaises(ValueError):
            engine.set_num_processes(0)

    def test_locked_engine_error(self) -> None:
        """Test a ``RuntimeError`` is raised when setting processes on a locked engine"""

        engine = MultiprocessingEngine(num_processes=4, target=lambda: 1)
        engine.run()

        with self.assertRaises(RuntimeError):
            engine.set_num_processes(2)


class Reset(TestCase):
    """Tests for the ``reset`` method"""

    def test_num_processes_settable(self) -> None:
        """Test the number of processes becomes settable after execution"""

        engine = MultiprocessingEngine(num_processes=4, target=lambda: 1)
        engine.run()
        engine.reset()

        engine.set_num_processes(2)
        self.assertEqual(2, engine.get_num_processes())

    def test_not_executed_error(self) -> None:
        """Test a ``RuntimeError`` is raised when calling the method before executing the engine"""

        engine = MultiprocessingEngine(num_processes=4, target=lambda: 1)
        with self.assertRaises(RuntimeError):
            engine.reset()
