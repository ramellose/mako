"""
The netstats module contains functions for analysis of the graphs in the Neo4j database.
These analytical methods do not involve metadata.
"""

__author__ = 'Lisa Rottjers'
__maintainer__ = 'Lisa Rottjers'
__email__ = 'lisa.rottjers@kuleuven.be'
__status__ = 'Development'
__license__ = 'Apache 2.0'

import sys
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


def start_netstats(inputs):
    """
    Takes all arguments and processes these to carry out network analysis on the Neo4j database,
    where the specific type of network analysis does not require node metadata.

    :param inputs: Dictionary of arguments.
    :return:
    """
    _create_logger(inputs['fp'])
    config = _read_config(inputs)
    try:
        driver = NetstatsDriver(uri=config['address'],
                                user=config['username'],
                                password=config['password'],
                                filepath=inputs['fp'])
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
        driver.graph_union(networks=networks)
        for fraction in inputs['fraction']:
            driver.graph_intersection(networks=networks,
                                      weight=inputs['weight'], fraction=fraction)
        driver.graph_difference(networks=networks,
                                weight=inputs['weight'])
    driver.close()
    logger.info('Completed netstats operations!  ')


class NetstatsDriver(ParentDriver):
    """
    Initializes a driver for accessing the Neo4j database.
    This driver extracts nodes and edges from the database that are required
    for the operations defined in the netstats module.
    """
    def graph_union(self, networks=None):
        """
        Returns a subgraph that contains all nodes present in all networks.
        If network names are specified as a list, all nodes present
        in these two networks are returned.
        :param networks: List of network names
        :return: Edge list of lists containing source, target, network and weight of each edge.
        """
        union = None
        try:
            with self._driver.session() as session:
                union = session.read_transaction(self._get_union, networks)
                logger.info("The union set operation for networks " + str(networks) +
                            " has been added to "
                            "the database\nwith name " + union + ". ")
                size = session.read_transaction(self._get_size, union)
                logger.info("This union contains " + str(size) + " edges. ")
        except Exception:
            logger.error("Could not obtain graph union. ", exc_info=True)
        return union

    def graph_intersection(self, networks=None, weight=True, fraction=None):
        """
        Returns a subgraph that contains all nodes present in both specified networks.
        If no networks are specified, the function returns only nodes that are
        connected to all nodes in the network.
        :param networks: List of network names
        :param weight: If false, the intersection includes edges with matching partners but different weights
        :param fraction: If specified, fraction of networks that the intersecting node should be in
        :return: Edge list of lists containing source, target, network and weight of each edge.
        """
        intersection = None
        n = round(len(networks) * fraction)
        try:
            with self._driver.session() as session:
                intersection = session.read_transaction(self._get_intersection, networks, weight=weight, n=n)
                logger.info("The intersection set operation for networks " + str(networks) +
                            " has been added to "
                            "the database\nwith name " + intersection + ". ")
                size = session.read_transaction(self._get_size, intersection)
                logger.info("This intersection contains " + str(size) + " edges. ")
        except Exception:
            logger.error("Could not obtain graph intersection. ", exc_info=True)
        return intersection

    def graph_difference(self, networks=None, weight=True):
        """
        Returns a subgraph that contains all nodes only present in one of the selected networks.
        If no networks are specified, returns all edges that are unique across multiple networks.
        :param networks: List of network names
        :param weight: If false, the difference excludes edges with matching partners but different weights
        :return: Edge list of lists containing source, target, network and weight of each edge.
        """
        difference = None
        try:
            with self._driver.session() as session:
                difference = session.read_transaction(self._get_difference, networks, weight=weight)
                logger.info("The difference set operation for networks " + str(networks) +
                            " has been added to "
                            "the database \nwith name " + difference + ". ")
                size = session.read_transaction(self._get_size, difference)
                logger.info("This difference contains " + str(size) + " edges. ")
        except Exception:
            logger.error("Could not obtain graph difference. ", exc_info=True)
        return difference

    @staticmethod
    def _get_weight(tx, node):
        """
        Returns the weight of an Edge node.
        :param tx: Neo4j transaction
        :param node: Returns the weight of the specified node
        :return:
        """
        weight = tx.run("MATCH (n:Edge {name: '" + node.get('name') +
                        "'}) RETURN n").data()[0]['n'].get('weight')
        return weight

    @staticmethod
    def _get_union(tx, networks):
        """
        Accesses database to return edge list of union of networks.
        :param tx: Neo4j transaction
        :param networks: List of network names
        :return: Edge list of lists containing source, target, network and weight of each edge.
        """
        edges = tx.run(("WITH " + str(networks) +
                        " as names MATCH (n:Edge)-->(b:Network) "
                        "WHERE b.name in names RETURN n")).data()
        edges = _get_unique(edges, 'n')
        setname = _write_logic(tx, operation='Union', networks=networks, edges=edges)
        return setname

    @staticmethod
    def _get_intersection(tx, networks, weight, n):
        """
        Accesses database to return edge list of intersection of networks.
        :param tx: Neo4j transaction
        :param networks: List of network names
        :param weight: If false, the intersection includes edges with matching partners but different weights
        :param n: If specified, number of networks that the intersecting node should be in
        :return: Edge list of lists containing source, target, network and weight of each edge.
        """
        if not n:
            queries = list()
            for node in networks:
                queries.append(("MATCH (n:Edge)-->(:Network {name: '" +
                                node + "'}) "))
            query = " ".join(queries) + "RETURN n"
            edges = tx.run(query).data()
        else:
            edges = list()
            combos = combinations(networks, n)
            for combo in combos:
                queries = list()
                for node in combo:
                    queries.append(("MATCH (n:Edge)-->(:Network {name: '" +
                                    node + "'}) "))
                query = " ".join(queries) + "RETURN n"
                combo_edges = tx.run(query).data()
                edges.extend(combo_edges)
        edges = list(_get_unique(edges, 'n'))
        if weight:
            query = ("MATCH (a)-[:PARTICIPATES_IN]-(n:Edge)-[:PARTICIPATES_IN]-(b) "
                     "MATCH (a)-[:PARTICIPATES_IN]-(m:Edge)-[:PARTICIPATES_IN]-(b) "
                     "WHERE (n.name <> m.name) RETURN n, m")
            weighted = tx.run(query).data()
            filter_weighted = list()
            for edge in weighted:
                # check whether edges are in all networks
                in_networks = list()
                nets = tx.run(("MATCH (a:Edge {name: '"
                               + edge['n'].get('name') +
                               "'})-->(n:Network) RETURN n")).data()
                nets = _get_unique(nets, 'n')
                in_networks.extend(nets)
                nets = tx.run(("MATCH (a:Edge {name: '"
                               + edge['m'].get('name') +
                               "'})-->(n:Network) RETURN n")).data()
                nets = _get_unique(nets, 'n')
                in_networks.extend(nets)
                if n:
                    if len(in_networks) > n:
                        filter_weighted.append(edge)
                else:
                    if all(x in in_networks for x in networks):
                        filter_weighted.append(edge)
            edges.extend(_get_unique(filter_weighted, 'n'))
            edges.extend(_get_unique(filter_weighted, 'm'))
        name = 'Intersection'
        if weight:
            name += '_weight'
        if n:
            name = name + '_' + str(n)
        setname = _write_logic(tx, operation=name, networks=networks, edges=edges)
        return setname

    @staticmethod
    def _get_difference(tx, networks, weight):
        """
        Accesses database to return edge list of difference of networks.
        :param tx: Neo4j transaction
        :param networks: List of network names
        :param weight: If false, the difference excludes edges with matching partners but different weights
        :return: Edge list of lists containing source, target, network and weight of each edge.
        """
        edges = list()
        for network in networks:
            edges.extend(tx.run(("MATCH (n:Edge)-->(:Network {name: '" + network +
                                 "'}) WITH n MATCH (n)-[r]->(:Network) WITH n, count(r) "
                                 "as num WHERE num=1 RETURN n")).data())
        edges = _get_unique(edges, 'n')
        if weight:
            cleaned = list()
            for edge in edges:
                query = ("MATCH (a)-[:PARTICIPATES_IN]-(n:Edge {name: '" + edge +
                         "'})-[:PARTICIPATES_IN]-(b) "
                         "MATCH (a)-[:PARTICIPATES_IN]-(m:Edge)-[:PARTICIPATES_IN]-(b) "
                         "WHERE (n.name <> m.name) RETURN n, m")
                check = tx.run(query).data()
                if len(check) == 0:
                    cleaned.append(edge)
            edges = cleaned
        name = 'Difference'
        if weight:
            name += '_weight'
        setname = _write_logic(tx, operation=name, networks=networks, edges=edges)
        return setname

    @staticmethod
    def _get_size(tx, operation):
        """
        Returns the number of edges in a set.

        :param tx: Neo4j transaction
        :param operation: Name of set
        :return: Integer
        """
        num = tx.run("MATCH (n:Set {name: $id})-"
                     "[r:IN_SET]-() RETURN count(r) as count",
                     id=operation).data()
        return num[0]['count']


def _write_logic(tx, operation, networks, edges):
    """
    Accesses database to return edge list of intersection of networks.
    :param tx: Neo4j transaction
    :param operation: Type of logic operation
    :param networks: List of network names
    :param edges: List of edges returned by logic operation
    :return:
    """
    name = operation
    # first match and detach delete
    # then recreate association
    # prevents repeated intersections from overlappind
    tx.run("MATCH (n:Set {name: $id, networks: $networks}) "
           "DETACH DELETE n", id=name, networks=str(networks))
    tx.run("CREATE (n:Set {name: $id, networks: $networks}) "
           "RETURN n", id=name, networks=str(networks))
    for edge in edges:
        tx.run(("MATCH (a:Edge), (b:Set) WHERE a.name = '" +
                edge +
                "' AND b.name = '" + name +
                "' CREATE (a)-[r:IN_SET]->(b) "
                "RETURN type(r)"))
    return name