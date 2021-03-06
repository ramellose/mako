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
    if inputs['store_config']:
        config = _read_config(inputs)
    else:
        config = inputs
    driver = None
    try:
        driver = NetstatsDriver(uri=config['address'],
                                user=config['username'],
                                password=config['password'],
                                filepath=inputs['fp'],
                                encrypted=inputs['encryption'])
    except KeyError:
        logger.error("Login information not specified in arguments.", exc_info=True)
        exit()
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
                union_edges = session.read_transaction(self._get_union, networks)

            with self._driver.session() as session:
                setname = session.write_transaction(_write_logic, operation='Union',
                                                   networks=networks, edges=union_edges)
                logger.info("The union set operation for networks " + str(networks) +
                            " has been added to "
                            "the database\nwith name " + setname + ". ")
            with self._driver.session() as session:
                size = session.read_transaction(self._get_size, setname)
                logger.info("This union contains " + str(size) + " edges. ")
        except Exception:
            logger.error("Could not obtain graph union. ", exc_info=True)
        return union

    def graph_intersection(self, networks=None, weight=True, fraction=None):
        """
        Returns a subgraph that contains all nodes present in both specified networks.
        If no networks are specified, the function returns only nodes that are
        connected to all networks.
        :param networks: List of network names
        :param weight: If false, the intersection includes edges with matching partners but different weights
        :param fraction: If specified, fraction of networks that the intersecting node should be in
        :return: Edge list of lists containing source, target, network and weight of each edge.
        """
        intersection = None
        n = round(len(networks) * fraction)
        if n <= 1:
            logger.warning("Skipping intersection with 1 or fewer networks.")
        else:
            try:
                with self._driver.session() as session:
                    intersection_edges = session.read_transaction(self._get_intersection, networks, weight=weight, n=n)
                with self._driver.session() as session:
                    name = 'Intersection'
                    if weight:
                        name += '_weight'
                    if n:
                        name = name + '_' + str(n)
                    setname = session.write_transaction(_write_logic, operation=name,
                                                        networks=networks, edges=intersection_edges)
                logger.info("The intersection set operation for networks " + str(networks) +
                            " has been added to "
                            "the database\nwith name " + setname + ". ")
                with self._driver.session() as session:
                    size = session.read_transaction(self._get_size, setname)
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
                difference_edges = session.read_transaction(self._get_difference, networks, weight=weight)

            with self._driver.session() as session:
                name = 'Difference'
                if weight:
                    name += '_weight'
                setname = session.write_transaction(_write_logic, operation=name,
                                                   networks=networks, edges=difference_edges)
                logger.info("The difference set operation for networks " + str(networks) +
                            " has been added to "
                            "the database \nwith name " + setname + ". ")
            with self._driver.session() as session:
                size = session.read_transaction(self._get_size, setname)
                logger.info("This difference contains " + str(size) + " edges. ")
        except Exception:
            logger.error("Could not obtain graph difference. ", exc_info=True)
        return difference

    @staticmethod
    def _get_weight(tx, node, network):
        """
        Returns the weight of an Edge node.
        :param tx: Neo4j transaction
        :param node: Returns the weight of the specified node
        :param network: Network where weight is needed
        :return:
        """
        weight = tx.run("MATCH (n:Edge {name: '" + node.get('name') +
                        "'})-[r]-(:Network {name: '" + network + "'})"
                        " RETURN r").data()[0]['r']['weight']
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
        return edges

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
        # First, get a list of all edges
        curated_weighted_edges = []
        if not n:
            # get edges that are in all networks
            unweighted_query, weighted_query = _get_intersection_query(networks, weight)
            edges = tx.run(unweighted_query).data()
            if weighted_query:
                weighted_edges = tx.run(weighted_query).data()
                # ok not to match pattern,
                # it will always be captured in reverse too
                weighted_edges = _get_unique(weighted_edges, key='a')
                curated_weighted_edges = _curate_weighted_edges(tx, weighted_edges, networks)
        else:
            # get all edges in number of networks
            edges = list()
            combos = list(combinations(networks, n))
            for combo in combos:
                unweighted_query, weighted_query = _get_intersection_query(combo, weight)
                edges.extend(tx.run(unweighted_query).data())
                # the weighted query returns edges present in multiple networks
                # but it's not guaranteed to be all the networks in combo
                if weighted_query:
                    weighted_edges = tx.run(weighted_query).data()
                    weighted_edges = _get_unique(weighted_edges, key='a')
                    curated_weighted_edges = _curate_weighted_edges(tx, weighted_edges, networks)
        edges = list(_get_unique(edges, 'n'))
        edges.extend(curated_weighted_edges)
        name = 'Intersection'
        if weight:
            name += '_weight'
        if n:
            name = name + '_' + str(n)
        return edges

    @staticmethod
    def _get_difference(tx, networks, weight):
        """
        Accesses database to return edge list of difference of networks.
        :param tx: Neo4j transaction
        :param networks: List of network names
        :param weight: If false, the difference includes edges with matching partners but different weights
        :return: Edge list of lists containing source, target, network and weight of each edge.
        """
        edges = list()
        for network in networks:
            # all edges with only 1 link to a network
            # are part of the difference
            edges.extend(tx.run(("MATCH (n:Edge)-->(x:Network {name: '" + network +
                                 "'}) WITH n MATCH (n)-[r]->(y:Network) WHERE y.name IN " + str(networks) +
                                 " WITH n, count(r) "
                                 "as num WHERE num=1 RETURN n")).data())
        edges = _get_unique(edges, 'n')
        if weight:
            # if edges are in 2 networks,
            # and they have a different sign in each network,
            # they are removed in the weighted difference.
            # so query each combination of networks
            # to find edges that have a relationship to those networks,
            # with different weights.
            unweighted_query, weighted_query = _get_intersection_query(networks, weight=False)
            edge_partners = tx.run(weighted_query).data()
            edge_partners = _get_unique(edge_partners, key='a')
            curated_weighted_edges = _curate_weighted_edges(tx, edge_partners, networks)
            edges = edges.difference(curated_weighted_edges)
        name = 'Difference'
        if weight:
            name += '_weight'
        return edges

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


def _curate_weighted_edges(tx, weighted_edges, networks):
    """
    Takes a list with names of associations of weighted edges,
    and returns the networks connected to both edges.
    Weighted edges that are not connected to all networks in
    the networks parameter are not returned.
    :param weighted_edges:
    :param networks:
    :return:
    """
    curated_edges = list()
    query_edges = [{'name': x} for x in weighted_edges]
    query = "WITH $batch as batch " \
            "UNWIND batch as record " \
            "MATCH (m)--(a:Edge {name: record.name})--(n)--(b:Edge)--(m) " \
            "WHERE (m:Taxon OR m:Property) AND (n:Taxon OR n:Property) " \
            "RETURN a.name,b.name LIMIT 1"
    paired_edges = tx.run(query, batch=query_edges)
    query = "WITH $batch as batch " \
            "UNWIND batch as record " \
            "MATCH (a:Edge {name: record.name})--(b:Network) " \
            "RETURN a.name,b.name"
    network_edges = tx.run(query, batch=query_edges).data()
    network_dict = {x['a.name']: [] for x in network_edges}
    for edge in network_edges:
        network_dict[edge['a.name']].append(edge['b.name'])
    for pair in paired_edges:
        total_networks = network_dict[pair['a.name']] + network_dict[pair['b.name']]
        result = all(elem in total_networks for elem in networks)
        if result:
            curated_edges.extend(list(network_dict.keys()))
    return set(curated_edges)


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
    edges = [{'name': x, 'set': name} for x in edges]
    query = "WITH $batch as batch " \
            "UNWIND batch as record " \
            "MATCH (a:Edge {name: record.name}), " \
            "(b:Set {name: record.set}) " \
            "MERGE (a)-[r:IN_SET]->(b) " \
            "RETURN type(r)"
    tx.run(query, batch=edges)
    return name


def _get_intersection_query(networks, weight=True):
    """
    For a list of networks,
    constructs a query that gets edges belonging to all networks.
    Can extract only edges that have the same weight in all networks,
    or edges that have different weights (if weight is false.)
    :param networks: List of networks that edges should be in
    :param weight: If false, edges are counted if they are separate nodes but have the same partners
    :return:
    """
    queries = list()
    weighted_query = None
    for node in networks:
        queries.append(("MATCH (n:Edge)-->(:Network {name: '" +
                        node + "'}) "))
    if not weight:
        weighted_query = ("MATCH (m)--(a:Edge)--(n)--(b:Edge)--(m) "
                          "WHERE (m:Taxon OR m:Property) AND (n:Taxon OR n:Property) "
                          "WITH a, b MATCH (a)--(c:Network) "
                          "MATCH (b)--(d:Network) "
                          "WHERE c.name IN  " + str(list(networks)) +
                          " AND d.name IN " + str(list(networks)) +
                          " RETURN a,b,c,d ")
    query = " ".join(queries) + " RETURN n"
    return query, weighted_query



