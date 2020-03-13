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
from manta.cluster import cluster_graph
from manta.reliability import perm_clusters
from anuran.main import model_calcs
from mako.scripts.utils import ParentDriver, _get_unique, _create_logger, _read_config
import logging.handlers
import networkx as nx

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
    # get networks
    networks = driver.return_networks(inputs['networks'])
    # run manta
    if 'manta' in inputs:
        for network in networks:
            run_manta(inputs, name=network,
                      network=networks[network], driver=driver)
    if 'anuran' in inputs:
        run_anuran(inputs, networks, driver)
    # run anuran

    # add properties to database

    driver.close()
    logger.info('Completed netstats operations!  ')


def run_manta(inputs, name, network, driver):
    """
    Takes the extracted network object and runs manta.
    The manta results are then uploaded back to
    the database as node properties.
    :param inputs: Arguments for manta
    :param name: Name of network object
    :param network: Network to cluster
    :param driver: Neo4j IO driver
    :return:
    """
    network = nx.to_undirected(network)
    results = cluster_graph(network, limit=inputs['limit'], max_clusters=inputs['max'],
                            min_clusters=inputs['min'], min_cluster_size=inputs['ms'],
                            iterations=inputs['iter'], subset=inputs['subset'],
                            ratio=inputs['ratio'], edgescale=inputs['edgescale'],
                            permutations=inputs['perm'], verbose=True)
    graph = results[0]
    if inputs['cr']:
        perm_clusters(graph=graph, limit=inputs['limit'], max_clusters=inputs['max'],
                      min_clusters=inputs['min'], min_cluster_size=inputs['ms'],
                      iterations=inputs['iter'], ratio=inputs['ratio'],
                      partialperms=inputs['perm'], relperms=inputs['rel'], subset=inputs['subset'],
                      error=inputs['error'], verbose=True)
    # do something with writing to the database here


def run_anuran(inputs, networks, driver):
    """
    Takes the extracted network objects and runs anuran.
    The anuran centrality results are then uploaded back to
    the database as node properties.
    :param inputs: Arguments for anuran
    :param network: Network to cluster
    :param driver: Neo4j IO driver
    :return:
    """
    centralities = model_calcs(networks=networks, inputs=inputs)
    # do something with writing to the database here