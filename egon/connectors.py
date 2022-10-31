"""``Connector`` objects are responsible communicating information
between nodes. Individual connector instances are assigned to a
single parent node and can be used to send or receive data depending
on the type of connector. ``Output`` connectors are used to send data
and ``Input`` objects are used to receive data.
"""

from __future__ import annotations

from typing import Any, Optional, Tuple

from egon.exceptions import MissingConnectionError


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


class InputConnector:
    ...


class OutputConnector(BaseConnector):
    """Handles the output of data from a pipeline node"""

    def __init__(self, name: str = None) -> None:
        """Create a new connector instance

        Args:
            name: Optional name for the connector object
        """

        super().__init__(name=name)
        self._partner: list[InputConnector] = []

    def connect(self, conn: InputConnector) -> None:
        """Establish the flow of data between this connector and an ``InputConnector`` instance

        Args:
            conn: The input connector object ot connect with
        """

    def disconnect(self, conn: InputConnector) -> None:
        """Disconnect any established connection to the given ``InputConnector`` instance

        Args:
            conn: The input connector to disconnect from
        """

    def put(self, item: Any) -> None:
        """Add an item to the connector queue

        Args:
            item: The item to add to the connector

        Raises:
            MissingConnectionError: If trying to put data into an output that isn't connected to an input
        """

        if not self.is_connected():
            raise MissingConnectionError('Output connector is not connected to any input connectors.')

        for partner in self.partners:
            partner._put(item)
