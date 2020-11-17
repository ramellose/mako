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
    try:
        driver = IoDriver(uri=config['address'],
                          user=config['username'],
                          password=config['password'],
                          filepath=inputs['fp'],
                          encrypted=inputs['encryption'])
    except KeyError:
        logger.error("Login information not specified in arguments.", exc_info=True)
        exit()
    # get networks
    networks = driver.return_networks(inputs['networks'])
    # run manta
    if 'manta' in inputs:
        for network in networks:
            run_manta(inputs, network=networks[network], network_id=network, driver=driver)
    if 'anuran' in inputs:
        run_anuran(inputs, networks, driver)
    driver.close()
    logger.info('Completed netstats operations!  ')


def run_manta(inputs, network, network_id, driver):
    """
    Takes the extracted network object and runs manta.
    The manta results are then uploaded back to
    the database as node properties.
    :param inputs: Arguments for manta
    :param network: Network to cluster
    :param network_id: Network ID
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
        for node in nodes:
            node_dict = construct_manta(source=node, value=nodes[node], network_id=network_id)
            driver.include_nodes(node_dict, name='Cluster', label='Taxon', verbose=False)
    nodes = nx.get_node_attributes(graph, 'cluster')
    for node in nodes:
        node_dict = construct_manta(source=node, value=nodes[node], network_id=network_id)
        driver.include_nodes(node_dict, name='Cluster', label='Taxon', verbose=False)

    if inputs['cr']:
        perm_clusters(graph=graph, limit=inputs['limit'], max_clusters=inputs['max'],
                      min_clusters=inputs['min'], min_cluster_size=inputs['ms'],
                      iterations=inputs['iter'], ratio=inputs['ratio'],
                      partialperms=inputs['perm'], relperms=inputs['rel'], subset=inputs['subset'],
                      error=inputs['error'], verbose=False)
        nodes = nx.get_node_attributes(graph, 'lowerCI')
        for node in nodes:
            node_dict = construct_manta(source=node, value=nodes[node], network_id=network_id)
            driver.include_nodes(node_dict, name='lowerCI', label='Taxon', verbose=False)
        nodes = nx.get_node_attributes(graph, 'upperCI')
        for node in nodes:
            node_dict = construct_manta(source=node, value=nodes[node], network_id=network_id)
            driver.include_nodes(node_dict, name='upperCI', label='Taxon', verbose=False)
        nodes = nx.get_node_attributes(graph, 'widthCI')
        for node in nodes:
            node_dict = construct_manta(source=node, value=nodes[node], network_id=network_id)
            driver.include_nodes(node_dict, name='widthCI', label='Taxon', verbose=False)


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
    args['network'] = args['graph']
    centralities = model_calcs(networks={'Neo4j': grouped_networks}, args=args)
    # add a node with the different centrality
    # give node properties with comparison value
    # add p value as relationship
    if centralities:
        for index, row in centralities.iterrows():
            centrality_dict = {}
            centrality_dict[row['Node']] = {}
            centrality_dict[row['Node']]['target'] = row['Measure'] + ', ' + row['Comparison']
            centrality_dict[row['Node']]['rel_property'] = [('pvalue', row['P'])]
            if 'P.adj' in row:
                centrality_dict[row['Node']]['rel_property'].append(('padj', row['P.adj']))
            driver.include_nodes(centrality_dict, name='Centrality', label='Taxon', verbose=False)


def construct_manta(source, value, network_id):
    """
    Constructs a dictionary from manta outputs that
    can be imported into the Neo4j database.
    :param source:
    :param value:
    :param network_id:
    :return:
    """
    node_dict = {}
    node_dict[source] = {}
    node_dict[source]['target'] = network_id + ', cluster ' + str(value)
    return node_dict
