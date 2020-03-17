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
from mako.scripts.utils import _create_logger, _read_config
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
            run_manta(inputs, network=networks[network], driver=driver)
    if 'anuran' in inputs:
        run_anuran(inputs, networks, driver)
    driver.close()
    logger.info('Completed netstats operations!  ')


def run_manta(inputs, network, driver):
    """
    Takes the extracted network object and runs manta.
    The manta results are then uploaded back to
    the database as node properties.
    :param inputs: Arguments for manta
    :param network: Network to cluster
    :param driver: Neo4j IO driver
    :return:
    """
    network = nx.to_undirected(network)
    results = cluster_graph(network, limit=inputs['limit'], max_clusters=inputs['max'],
                            min_clusters=inputs['min'], min_cluster_size=inputs['ms'],
                            iterations=inputs['iter'], subset=inputs['subset'],
                            ratio=inputs['ratio'], edgescale=inputs['edgescale'],
                            permutations=inputs['perm'], verbose=False)
    graph = results[0]
    # write to db
    nodes = nx.get_node_attributes(graph, 'assignment')
    if len(nodes) > 0:
        # sometimes no assignment if the graph is balanced
        driver.include_nodes(nodes, name='assignment', label='Taxon')
    nodes = nx.get_node_attributes(graph, 'cluster')
    driver.include_nodes(nodes, name='cluster', label='Taxon')
    if inputs['cr']:
        perm_clusters(graph=graph, limit=inputs['limit'], max_clusters=inputs['max'],
                      min_clusters=inputs['min'], min_cluster_size=inputs['ms'],
                      iterations=inputs['iter'], ratio=inputs['ratio'],
                      partialperms=inputs['perm'], relperms=inputs['rel'], subset=inputs['subset'],
                      error=inputs['error'], verbose=False)
        nodes = nx.get_node_attributes(graph, 'lowerCI')
        driver.include_nodes(nodes, name='lowerCI', label='Taxon')
        nodes = nx.get_node_attributes(graph, 'upperCI')
        driver.include_nodes(nodes, name='upperCI', label='Taxon')
        nodes = nx.get_node_attributes(graph, 'widthCI')
        driver.include_nodes(nodes, name='widthCI', label='Taxon')


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
    grouped_networks = [(name, networks[name]) for name in networks]
    args = inputs.copy()
    args['fp'] += '/anuran'
    centralities = model_calcs(networks={'Neo4j': grouped_networks}, args=args)
    # add a node with the different centrality
    # give node properties with comparison value
    # add p value as relationship
    for index, row in centralities.iterrows():
        # still need to fix:
        # only random centralities added
        centrality_dict = {}
        centrality_dict[row['Node']] = {}
        centrality_dict[row['Node']]['target'] = row['Measure'] + ', ' + row['Comparison']
        centrality_dict[row['Node']]['rel_property'] = [('pvalue', row['P']),
                                                        ('padj', row['P.adj'])]
        driver.include_nodes(centrality_dict, name='Centrality', label='Taxon', verbose=False)