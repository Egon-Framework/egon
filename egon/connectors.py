"""``Connector`` objects are responsible communicating information
between nodes. Individual connector instances are assigned to a
single parent node and can be used to send or receive data depending
on the type of connector. ``Output`` connectors are used to send data
and ``Input`` objects are used to receive data.
"""

from __future__ import annotations

import multiprocessing as mp
from queue import Empty
from typing import Any, Optional, Tuple


class BaseConnector:
    """Base class for building signal/slot style connectors on top of an underlying queue"""

    def __init__(self, name: str = None) -> None:
        """Queue-like object for passing data between nodes

        By default, connector names are generated using the instances memory
        identify in hexadecimal representation.

        Args:
            name: Optional name for the connector object
        """

        # Identifying information for the instance
        self._id = hex(id(self))
        self.name = str(self._id) if name is None else name

        # The parent node
        self._node = None

        # Other connector objects connected to this instance
        self._connected_partners = []

    @property
    def parent_node(self) -> Optional:  # Todo: update this type hint
        """The parent node this connector is assigned to"""

        return self._node

    @property
    def partners(self) -> Tuple:
        """Return a tuple of connectors that are connected to this instance"""

        return tuple(self._connected_partners)

    def is_connected(self) -> bool:
        """Return whether the connector has any established connections"""

        return bool(self._connected_partners)

    def __str__(self) -> str:
        """Return the name of the parent instance"""

        return f'<{self.__class__.__name__}(name={self.name}) object at {self._id}>'


class InputConnector(BaseConnector):
    """A queue-like object for handling input data into a node object

    The interface for this class is designed to mimic (but not replace)
    the built-in ``multiprocessing.Queue`` class.
    """

    def __init__(self, name: str = None, maxsize: int = None) -> None:
        """Create a new input connector

        Args:
            name: Optional name for the connector object
            maxsize: The maximum number of items to store in connector at once
        """

        super().__init__(name)
        self._queue = mp.Manager().Queue(maxsize=maxsize or 0)

    def empty(self) -> bool:
        """Return whether the parent connector instance is empty"""

        return self._queue.empty()

    def full(self) -> bool:
        """Return whether the parent connector instance is full"""

        return self._queue.full()

    def size(self) -> int:
        """Return the number of items currently stored in the parent instance"""

        return self._queue.qsize()

    @property
    def maxsize(self) -> Optional[int]:
        """The maximum number of objects to store in the connector at once"""

        return self._queue.maxsize

    def put(self, item: Any) -> None:
        """Add an item to the connector queue

        Args:
            item: The item to add to the connector
        """

        self._queue.put(item)

    def get(self, timeout: Optional[int] = None, refresh_interval: int = 2) -> Any:
        """Retrieve data from the instance queue

        This is a blocking method and cannot be called asynchronously.
        Open calls to this method will return automatically should the
        upstream node object close mid-call.

        Args:
            timeout: Optionally raise a ``TimeoutError`` if data is not retrieved within the given number of seconds
            refresh_interval: How often to check if data is expected from upstream

        Raises:
            TimeOutError: Raised if the method call times out
        """

        if refresh_interval <= 0:
            raise ValueError('Connector refresh interval must be greater than zero.')

        if timeout is None:
            timeout = float('inf')

        while timeout > 0:
            this_timeout = min(timeout, refresh_interval)
            try:
                return self._queue.get(timeout=this_timeout)

            except (Empty, TimeoutError):
                timeout -= this_timeout
                if self.parent_node and self.parent_node.is_expecting_data():
                    continue

                raise

        raise TimeoutError

    def iter_get(self, timeout: Optional[int] = None, refresh_interval: int = 2) -> Any:
        """Iterator over data from the instance queue

        Similar to the ``get`` method, but data is returned as an iterable.

        Args:
            timeout: Raise a TimeoutError if data is not retrieved within the given number of seconds
            refresh_interval: How often to check if data is expected from upstream

        Raises:
            TimeOutError: Raised if the get call times out
        """

        while self.parent_node.expecting_data():
            try:
                yield self.get(timeout=timeout, refresh_interval=refresh_interval)

            except Empty as excep:
                raise StopIteration from excep
