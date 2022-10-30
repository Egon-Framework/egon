"""``Connector`` objects are responsible communicating information
between nodes. Individual connector instances are assigned to a
single parent node and can be used to send or receive data depending
on the type of connector. ``Output`` connectors are used to send data
and ``Input`` objects are used to receive data.
"""

from __future__ import annotations

from typing import Tuple


class BaseConnector:
    """Base class for building signal/slot style connectors on top of an underlying queue"""

    def __init__(self, name: str = None) -> None:
        """Queue-like object for passing data between nodes

        Args:
            name: Optional human-readable name for the connector object
        """

        self._id = hex(id(self))
        self.name = str(id(self)) if name is None else name

    @property
    def parent_node(self):
        """The parent node this connector is assigned to"""

    @property
    def partners(self) -> Tuple:
        """Return a tuple of connectors that are connected to this instance"""

    def is_connected(self) -> bool:
        """Return whether the connector has any established connections"""

    def __str__(self) -> str:
        """Return the name of the parent instance"""

        return f'<{self.__class__.__name__}(name={self.name}) object at {self._id}>'
