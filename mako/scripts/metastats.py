"""
The metastats module contains functions for analysis of the graphs in the Neo4j database.
These analytical methods involve metadata, such as taxonomy.
"""

__author__ = 'Lisa Rottjers'
__maintainer__ = 'Lisa Rottjers'
__email__ = 'lisa.rottjers@kuleuven.be'
__status__ = 'Development'
__license__ = 'Apache 2.0'

import os
import sys
from uuid import uuid4
from itertools import combinations
from mako.scripts.utils import ParentDriver, _get_unique, _create_logger, _read_config
import logging.handlers

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# handler to sys.stdout
sh = logging.StreamHandler(sys.stdout)
sh.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
sh.setFormatter(formatter)
logger.addHandler(sh)


def start_metastats(inputs):
    """
    Takes all arguments and processes these to carry out network analysis on the Neo4j database,
    where the specific type of network analysis requires node metadata.

    :param inputs: Dictionary of arguments.
    :return:
    """
    _create_logger(inputs['fp'])
    config = _read_config(inputs)
    try:
        driver = MetastatsDriver(uri=config['address'],
                                user=config['username'],
                                password=config['password'],
                                filepath=inputs['fp'])
    except KeyError:
        logger.error("Login information not specified in arguments.", exc_info=True)
        exit()


class MetastatsDriver(ParentDriver):
    """
    Initializes a driver for accessing the Neo4j database.
    This driver extracts nodes and edges from the database that are required
    for the operations defined in the metastats module.
    """

