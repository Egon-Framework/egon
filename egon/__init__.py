"""A Python parallelization framework for easily building powerful data analysis networks."""

from .connectors import InputConnector, OutputConnector
from .nodes import Node
from .pipelines import Pipeline

import logging
logging.basicConfig(level=logging.DEBUG)

__version__ = '0.0.0'
