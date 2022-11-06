"""Utilities for managing parallel execution, including process pools and queues."""

from __future__ import annotations

import multiprocessing as mp


class MultiprocessingEngine:
    """A processes pool for executing callable objects in parallel"""

    def __init__(self, num_processes: int, target: callable) -> None:
        """Create a new engine instance for evaluating the given callable

        Args:
            num_processes: The number of new processes to run in parallel
            target: The callable object to be executed in parallel
        """

        self._target = target
        self._processes = []  # Collection of processes managed by the parent instance
        self._states = mp.Manager().dict()  # Map process memory id to execution state

        self.set_num_processes(num_processes)

    def reset(self) -> None:
        """Reset the engine instance so it can be reused"""

    def get_num_processes(self) -> int:
        """Return the number of processes assigned to the pool"""

        return len(self._processes)

    def set_num_processes(self, num_processes: int) -> None:
        """Modify the number of processes assigned to the pool

        Args:
            num_processes: The number of processes to allocate
        """

        if num_processes <= 0:
            raise ValueError('Number of processes must be greater than zero')

        self._processes = [mp.Process(target=self._target) for _ in range(num_processes)]
        self._states.update({id(p): False for p in self._processes})

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
