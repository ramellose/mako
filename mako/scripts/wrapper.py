"""
The wrapper module contains functions for running manta and anuran.
Since these software tools have their own command line interface,
the command line interface for mako
"""

__author__ = 'Lisa Rottjers'
__maintainer__ = 'Lisa Rottjers'
__email__ = 'lisa.rottjers@kuleuven.be'
__status__ = 'Development'
__license__ = 'Apache 2.0'

import sys
from mako.scripts.io import IoDriver
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


def start_wrapper(inputs):
    """
    Extracts networks from the Neo4j database
    and runs manta or anuran on them.
    The manta clustering assignment
    and significant node centralities are ported back to the Neo4j database
    as node properties.

    :param inputs: Dictionary of arguments.
    :return:
    """
    _create_logger(inputs['fp'])
    if inputs['store_config']:
        config = _read_config(inputs)
    else:
        config = inputs
    encrypted = True
    driver = None
    if 'encryption' in inputs:
        # setting for Docker container
        encrypted = False
    try:
        driver = IoDriver(uri=config['address'],
                          user=config['username'],
                          password=config['password'],
                          filepath=inputs['fp'],
                          encrypted=encrypted)
    except KeyError:
        logger.error("Login information not specified in arguments.", exc_info=True)
        exit()
    # Only process ne
    if inputs['set']:
        if not inputs['networks']:
            networks = list()
            hits = driver.query("MATCH (n:Network) RETURN n")
            for hit in hits:
                networks.append(hit['n'].get('name'))
        else:
            networks = inputs['networks']
        driver.graph_union(networks=networks)
        for fraction in inputs['fraction']:
            driver.graph_intersection(networks=networks,
                                      weight=inputs['weight'], fraction=fraction)
        driver.graph_difference(networks=networks,
                                weight=inputs['weight'])
    driver.close()
    logger.info('Completed netstats operations!  ')