"""Utilities for managing parallel execution, including process pools and queues."""

from __future__ import annotations


class MultiprocessingEngine:
    """A processes pool for executing callable objects in parallel"""

    def __init__(self, num_processes: int, target: callable) -> None:
        """Create a new engine instance for evaluating the given callable

        Args:
            num_processes: The number of new processes to run in parallel
            target: The callable object to be executed in parallel
        """

    def reset(self) -> None:
        """Reset the engine instance so it can be reused"""

    def get_num_processes(self) -> int:
        """Return the number of processes assigned to the pool"""

    def set_num_processes(self, val: int) -> int:
        """Modify the number of processes assigned to the pool

        Args:
            val: The number of processes to allocate
        """

    def is_finished(self) -> bool:
        """Return whether all processes in the pool have exited execution"""

    def run(self) -> None:
        """Start all processes asynchronously"""

    def run_async(self) -> None:
        """Start all processes and join them to the current process"""

    def join(self) -> None:
        """Wait for any running processes to exit before continuing execution"""

    def kill(self) -> None:
        """Kill all running processes without attempting to exit gracefully"""
