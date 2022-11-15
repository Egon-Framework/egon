"""``Connector`` objects are responsible for communicating information
between analysis nodes. Individual connector instances are assigned to a
single parent node and can be used to send or receive data depending
on the connector type.

``OutputConnector`` objects are used to send data. ``InputConnector`` objects
are used to receive data. Together, the two connector types implement a
single/slot style interface for connecting analysis nodes together.
"""

from __future__ import annotations

import multiprocessing as mp
from queue import Empty
from typing import Any, Optional, Set, Tuple

from egon.exceptions import MissingConnectionError


class BaseConnector:
    """Base class for building signal/slot style connectors on top of an underlying queue"""

    def __init__(self, name: str = None) -> None:
        """Queue-like object for passing data between nodes

        By default, connector names are generated using the instance's memory
        identifier in hexadecimal representation.

        Args:
            name: Set a descriptive name for the connector object
        """

        # Identifying information for the instance
        self._id = hex(id(self))
        self.name = str(name) if name else str(self._id)

        # The parent node
        self._node = None

        # Other connector objects connected to this instance
        self._connected_partners: Set[BaseConnector] = set()

    @property
    def parent_node(self) -> Optional:  # Todo: update this type hint
        """The parent node this connector is assigned to"""

        return self._node

    def _add_partner(self, other: BaseConnector) -> None:
        """Add a partner connector

        Args:
            other: The connector to add
        """

        self._connected_partners.add(other)

    def _remove_partner(self, other: BaseConnector) -> None:
        """Remove a partner connector

        Args:
            other: The connector to remove
        """

        self._connected_partners.remove(other)

    @property
    def partners(self) -> Tuple[BaseConnector]:
        """Return a tuple of connectors that are connected to this instance"""

        return tuple(self._connected_partners)

    def is_connected(self) -> bool:
        """Return whether the connector has any established connections"""

        return bool(self._connected_partners)

    def __repr__(self) -> str:
        """Return a string representation of the parent class"""

        return f'<{self.__class__.__name__}(name={self.name}) object at {self._id}>'


class InputConnector(BaseConnector):
    """A queue-like object for handling input data into a node object

    The interface for this class is designed to mimic (but not replace)
    the built-in ``multiprocessing.Queue`` class.
    """

    def __init__(self, name: str = None, maxsize: int = 0) -> None:
        """Create a new input connector

        By default, the input object has no maximum size and can grow unbounded.

        Args:
            name: Set a descriptive name for the connector object
            maxsize: The maximum number of items to store in the connector at once
        """

        super().__init__(name)

        maxsize = maxsize or 0
        if not isinstance(maxsize, int) or maxsize < 0:
            raise ValueError('Maximum queue size must be a non-negative integer')

        self._maxsize = maxsize
        self._queue = mp.Manager().Queue(maxsize=maxsize)

    def empty(self) -> bool:
        """Return whether the connector is empty"""

        return self._queue.empty()

    def full(self) -> bool:
        """Return whether the connector is full"""

        return self._queue.full()

    def size(self) -> int:
        """Return the number of items currently stored in the connector"""

        return self._queue.qsize()

    @property
    def maxsize(self) -> Optional[int]:
        """The maximum number of objects to store in the connector at once"""

        return self._maxsize

    def _put(self, item: Any) -> None:
        """Add an item to the connector

        Args:
            item: The item to add to the connector
        """

        self._queue.put(item)

    def get(self, timeout: Optional[int] = None, refresh_interval: int = 2) -> Any:
        """Retrieve data from the connector

        This is a blocking method and cannot be called asynchronously.
        Open calls to this method will return automatically if all upstream
        node objects close mid-call.

        Args:
            timeout: Raise a ``TimeoutError`` if data is not retrieved within the given number of seconds
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
        """Iterate over data from the instance queue

        Similar to the ``get`` method, but data is returned as an iterable.

        Args:
            timeout: Raise a ``TimeoutError`` if data is not retrieved within the given number of seconds
            refresh_interval: How often to check if data is expected from upstream

        Raises:
            MissingConnectionError: When the connector is not assigned to a parent node
            TimeOutError: Raised if the method call times out
            StopIteration: When there is no more data to iterate over
        """

        if self.parent_node is None:
            raise MissingConnectionError(
                'The ``iter_get`` method cannot be used for ``InputConnector`` instances not assigned to a parent node.'
            )

        while self.parent_node.expecting_data():
            try:
                yield self.get(timeout=timeout, refresh_interval=refresh_interval)

            except Empty as excep:
                raise StopIteration from excep


class OutputConnector(BaseConnector):
    """Handles the output of data from a pipeline node"""

    def connect(self, conn: InputConnector) -> None:
        """Establish the flow of data between this connector and an ``InputConnector`` instance

        Args:
            conn: The input connector to connect with

        Raises:
            ValueError: When attempting to connect two output connectors
        """

        if type(conn) is type(self):
            raise ValueError('Cannot join together two connector objects of the same type.')

        self._add_partner(conn)
        conn._add_partner(self)

    def disconnect(self, conn: InputConnector) -> None:
        """Disconnect an established connection to the given ``InputConnector`` instance

        Args:
            conn: The input connector to disconnect from

        Raises:
            MissingConnectionError: When disconnecting two connectors that are not connected
        """

        if conn not in self._connected_partners:
            raise MissingConnectionError('The given connector object is not connected to this instance')

        # Disconnect both connectors from each other
        conn._remove_partner(self)
        self._remove_partner(conn)

    def put(self, item: Any) -> None:
        """Add an item to the connector queue

        Args:
            item: The item to add to the connector

        Raises:
            MissingConnectionError: When putting data into an output that isn't connected to an input
        """

        if not self.is_connected():
            raise MissingConnectionError('This output connector is not connected to any input connectors.')

        for partner in self._connected_partners:
            partner._put(item)
