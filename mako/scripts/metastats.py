"""
The metastats module contains functions for analysis of the graphs in the Neo4j database.
These analytical methods involve metadata, such as taxonomy.

Since the edge agglomeration requires the pair list to be updated in between steps,
this is one of the few larger queries that cannot be carried out as a batch operation.
"""

__author__ = 'Lisa Rottjers'
__maintainer__ = 'Lisa Rottjers'
__email__ = 'lisa.rottjers@kuleuven.be'
__status__ = 'Development'
__license__ = 'Apache 2.0'

import sys
from uuid import uuid4
import numpy as np
from mako.scripts.utils import ParentDriver, _get_unique, _create_logger, _read_config
import logging.handlers
from scipy.stats import hypergeom, spearmanr

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
    if inputs['store_config']:
        config = _read_config(inputs)
    else:
        config = inputs
    driver = None
    try:
        driver = MetastatsDriver(uri=config['address'],
                                 user=config['username'],
                                 password=config['password'],
                                 filepath=inputs['fp'],
                                 encrypted=inputs['encryption'])
    except KeyError:
        logger.error("Login information not specified in arguments.", exc_info=True)
        exit()
    if inputs['agglom']:
        tax_list = ['Species', 'Genus', 'Family', 'Order', 'Class', 'Phylum', 'Kingdom']
        level_id = tax_list.index(inputs['agglom'].capitalize())
        if not inputs['networks']:
            networks = list()
            hits = driver.query("MATCH (n:Network) RETURN n")
            for hit in hits:
                networks.append(hit['n'].get('name'))
        else:
            networks = inputs['networks']
        for level in range(0, level_id + 1):
            # pub.sendMessage('update', msg="Agglomerating edges...")
            logger.info("Checking " + tax_list[level] + " level...")
            networks = driver.agglomerate_networks(level=tax_list[level], weight=inputs['weight'], networks=networks)
            # networks assignment contains names of new networks
    if inputs['variable']:
        logger.info("Associating samples...  ")
        # sys.stdout.write("Associating samples...")
        variables = inputs['variable']
        if inputs['variable'][0] == 'all':
            variables = set([x[y] for x in driver.query("MATCH (n:Property) RETURN n.name") for y in x])
        for var in variables:
            driver.associate_samples(label=var)
    driver.close()
    logger.info('Completed metastats operations!  ')


class MetastatsDriver(ParentDriver):
    """
    Initializes a driver for accessing the Neo4j database.
    This driver extracts nodes and edges from the database that are required
    for the operations defined in the metastats module.
    """
    def agglomerate_networks(self, level=None, weight=True, networks=None):
        """
        Agglomerates to specified taxonomic level, or, if no level is specified,
        over all levels. Edges are agglomerated based on similarity
        at the specified taxonomic level. If 'weight' is set to True,
        edges are only agglomerated if their weight matches.
        The stop condition is the length of the pair list;
        as soon as no pair meets the qualification, agglomeration is terminated.
        By default, agglomeration is done separately
        per network in the database, so each network gets an agglomerated version.

        The networks parameter can be both a dict and a list.
        If it is a dict, the keys are the new network names, the values the old names.

        Pseudocode representation:
        1. Duplicate networks
        2. For each edge pair (taxon-level)-taxon-taxon-(taxon-level)
            3. Create new edge
            4. Delete edge pair

        :param level: Taxonomic level matching taxonomic assignments in Neo4j database
        :param weight: if True, takes edge weight into account
        :param networks: If specified, only these networks are agglomerated
        :return:
        """
        tax_list = ['Species', 'Genus', 'Family', 'Order', 'Class', 'Phylum', 'Kingdom']
        new_networks = dict()
        if not networks:
            networks = list()
            hits = self.query("MATCH (n:Network) RETURN n")
            for hit in hits:
                networks.append(hit['n'].get('name'))
        # we create a copy of the original network
        for network in networks:
            previous_network = network
            if type(networks) == list:
                new_name = level + '_' + network
            else:
                new_name = level + '_' + '_'.join(network.split('_')[1:])
            new_networks[new_name] = network
            # first check if lower-level network exists
            # if there were no pairs, it might not have been copied
            hit = self.query("MATCH (n:Network {name: '" + network + "'}) RETURN n")
            i = tax_list.index(level)
            while len(hit) == 0:
                previous_network = '_'.join(network.split('_')[1:])
                if i > 0:
                    previous_network = tax_list[i] + '_' + previous_network
                    i -= 1
                hit = self.query("MATCH (n:Network {name: '" + previous_network + "'}) RETURN n")
            # if there are no pairs at all, no need to copy network
            # possible with nodes that do not have large taxonomy
            testpair = self.get_pairlist(level, weight, previous_network)
            if len(testpair) == 0:
                testpair = self.get_taxlist(level, previous_network)
            if not len(testpair) == 0:
                logger.info("Copying " + previous_network + "...")
                self.copy_network(previous_network, new_name)
            else:
                new_networks[new_name] = None
        try:
            for network in new_networks:
                if new_networks[network]:
                    logger.info("Agglomerating " + network + "...")
                    stop_condition = False
                    while not stop_condition:
                        # limit is necessary to prevent excessively long queries
                        pairs = self.get_pairlist(level=level, weight=weight, network=network)
                        if pairs:
                            pair = pairs[0]['p']
                            self.agglomerate_pair(pair, level=level, weight=weight, network=network)
                        else:
                            stop_condition = True
                    stop_condition = False
                    while not stop_condition:
                        # after agglomerating edges
                        # taxa with same taxonomic assignments should be merged
                        # this rewires the network
                        tax_nodes = self.get_taxlist(level=level, network=network)
                        if tax_nodes:
                            tax_nodes = tax_nodes[0]['p']
                            self.agglomerate_taxa(tax_nodes, level=level, weight=weight)
                        else:
                            stop_condition = True
                    num = self.query("MATCH (n:Network {name: '" + network + "'})-"
                                     "[r:PART_OF]-() RETURN count(r) as count")
                    logger.info("The agglomerated network " + network +
                                " contains " + str(num) + " edges.")
        except Exception:
            logger.error("Could not agglomerate edges to higher taxonomic levels. \n", exc_info=True)
        return new_networks

    def copy_network(self, source_network, new_network):
        """
        Copies a network node and its edges.
        The network node name is new_network, edge IDs are generated with uuid4.
        The weights of the edges are not copied, only the signs.

        :param source_network: Source network name
        :param new_network: New network name
        :return:
        """
        self.query("MERGE (a:Network {name: '" + new_network + "'}) RETURN a")
        self.query("MATCH (a:Network {name: '" + new_network +
                   "'}), (b:Network {name: '" + source_network +
                   "'}) MERGE (a)-[r:AGGLOMERATED]->(b) RETURN r")
        edges = self.query("MATCH (a:Edge)--(:Network {name: '" + source_network +
                           "'}) RETURN a")
        edge_weights = dict()
        for edge in edges:
            edge_weights[edge['a']['name']] = edge['a']['weight']
        # Create dictionary with edges to write as batch query
        edge_names = [{'name': x['a']['name'], 'source': source_network, 'new': new_network, 'uid': str(uuid4())} for x in edges]
        with self._driver.session() as session:
            edge_partners = session.read_transaction(self._get_partners, edge_names)
        try:
            for edge in edge_names:
                name = edge['name']
                edge['a'] = edge_partners[name][0]
                edge['b'] = edge_partners[name][1]
                edge['weight'] = edge_weights[name]
        except Exception:
            logger.error("Could not include node pairs. \n", exc_info=True)
        with self._driver.session() as session:
            session.write_transaction(self._copy_edges, edge_names)

    def get_pairlist(self, level, weight, network):
        """
        Starts a new transaction for every pair list request.
        A pair is defined as two edges linked to taxonomic nodes
        that have the same taxonomic assignment at the specified level,
        e.g. Nitrobacter-edge-Nitrosomonas.
        :param level: Taxonomic level to identify a pair.
        :param weight: if True, specifies that edge weights should have the same sign.
        :param network: Name of network that the pairs should belong to
        :return: List containing results of Neo4j transaction
        """
        pairs = None
        try:
            with self._driver.session() as session:
                pairs = session.read_transaction(self._pair_list, level, weight, network)
        except Exception:
            logger.error("Could not obtain list of matching edges. \n", exc_info=True)
        return pairs

    def agglomerate_pair(self, pair, level, weight, network):
        """
        For one pair, as returned by get_pairlist,
        this function creates new agglomerated nodes,
        deletes old agglomerated nodes, and chains taxonomic nodes
        to the new agglomerated nodes. Morever, the two old edges
        are deleted and replaced by a new edge.
        :param pair: List containing transaction results of query for pair
        :param level: Taxonomic level to identify a pair.
        :param weight: if True, specifies that edge weights should have identical signs.
        :param network: Name of network
        :return:
        """
        try:
            with self._driver.session() as session:
                agglom_1 = session.write_transaction(self._create_agglom)
                agglom_2 = session.write_transaction(self._create_agglom)
                session.write_transaction(self._chainlinks, agglom_1, pair.nodes[1], pair.nodes[3])
                session.write_transaction(self._chainlinks, agglom_2, pair.nodes[5], pair.nodes[7])
                session.write_transaction(self._taxonomy, agglom_1, pair.nodes[2], level)
                session.write_transaction(self._taxonomy, agglom_2, pair.nodes[6], level)
            with self._driver.session() as session:
                if weight:
                    edge_sign = session.read_transaction(self._query, ("MATCH (n:Edge)-[r]-(m:Network) "
                                                                       "WHERE n.name = '" + pair.nodes[0]['name'] +
                                                                       "' AND m.name = '" + network +
                                                                       "' RETURN n.weight"))
                else:
                    edge_sign = None
                session.write_transaction(self._create_edge, agglom_1, agglom_2, network,
                                          edge_sign=edge_sign[0]['n.weight'])
            with self._driver.session() as session:
                session.write_transaction(self._delete_old_edges, [pair.nodes[0], pair.nodes[4]])
        except Exception:
            logger.error("Could not agglomerate a pair of matching edges. \n", exc_info=True)

    def associate_samples(self, label, null_input=None):
        """
        Sample identities themselves are not that informative,
        but the properties associated with them are.
        To test the hypothesis that taxa are associated with specific sample properties,
        the following tests are performed:
        1. For qualitative variables, a hypergeometric test is performed;
        how many edges do we expect by chance?
        2. For quantitative variables, Spearman correlation is performed.
        Because this is a hypothesis-generating tool,
        multiple-testing correction should be applied with care.
        :param label: Label of property (e.g. pH) to query.
        :param null_input: If missing values are not specified as NA, specify the NA input here.
        :return:
        """
        with self._driver.session() as session:
            label_nodes = session.read_transaction(self._query, "MATCH (n:Property) WHERE n.name = '" +
                                                   label + "' RETURN n.name")
        properties = list(set().union(*(d.values() for d in label_nodes)))
        try:
            with self._driver.session() as session:
                tax_nodes = session.read_transaction(self._query, "MATCH (n)--(:Edge) WHERE n:Taxon RETURN n")
                tax_nodes = _get_unique(tax_nodes, 'n')
            for node in tax_nodes:
                self.associate_taxon(taxon=node, null_input=null_input, properties=properties)
        except Exception:
            logger.error("Could not associate sample variables to taxa. \n", exc_info=True)

    def associate_taxon(self, taxon, null_input, properties):
        """
        Tests whether specific sample properties can be associated to a taxon.
        :param taxon: Name of a taxon.
        :param null_input: If missing values are not specified as NA, specify the NA input here.
        :param properties: List specifying types of properties to query.
        :return:
        """
        try:
            conts = list()
            categs = list()
            for property in properties:
                with self._driver.session() as session:
                    query = "MATCH (:Taxon {name: '" + taxon + \
                            "'})-->(:Specimen)-[r]->(n:Property {name: '" + property + \
                            "'}) RETURN n, r LIMIT 1"
                    rel = session.read_transaction(self._query, query)
                value = rel[0]['r'].get('value')
                if value == null_input:
                    break
                # try to convert value to float; if successful, adds type to continous vars
                try:
                    value = float(value)
                except ValueError:
                    pass
                if type(value) == float:
                    conts.append(property)
                else:
                    with self._driver.session() as session:
                        query = "MATCH (:Taxon {name: '" + taxon + \
                                "'})-->(:Specimen)-[r]->(n:Property {name: '" + property + \
                                "'}) RETURN r.value"
                        rel = session.read_transaction(self._query, query)
                    rel = set([x['r.value'] for x in rel])
                    for value in rel:
                        categs.append([property, value])
            conts = set(conts)
            categs = set(tuple(categ) for categ in categs)
            for categ_val in categs:
                with self._driver.session() as session:
                    hypergeom_vals = session.read_transaction(self._hypergeom_population, taxon, categ_val)
                    prob = hypergeom.cdf(hypergeom_vals['success_taxon'], hypergeom_vals['total_pop'],
                                         hypergeom_vals['success_pop'], hypergeom_vals['total_taxon'])
                    if prob < 0.05:
                        session.write_transaction(self._shortcut_categorical, taxon, categ_val, prob)
            for cont_val in conts:
                with self._driver.session() as session:
                    spearman_result = session.read_transaction(self._spearman_test,
                                                               taxon, cont_val)
                    if spearman_result.pvalue < 0.05:
                        var_dict = {cont_val: spearman_result.correlation}
                        session.write_transaction(self._shortcut_continuous, taxon, var_dict)
        except Exception:
            logger.error("Could not associate a specific taxon to sample variables. \n", exc_info=True)

    def get_taxlist(self, level, network):
        """
        Starts a new transaction for every tax list request.
        A tax list is a list containing two edges linked to identical taxa.
        :param level: Taxonomic level.
        :param network: Network name
        :return: List of transaction outcomes
        """
        pairs = None
        try:
            with self._driver.session() as session:
                pairs = session.read_transaction(self._tax_list, level, network)
        except Exception:
            logger.error("Could not obtain list of matching taxa. \n", exc_info=True)
        return pairs

    def agglomerate_taxa(self, pair, level, weight):
        """
        For one pair, as returned by get_taxlist,
        this function merges nodes with similar taxonomy
        but different edges together.
        Old nodes are linked to the new agglomerated node,
        except for Agglom_Taxon; in that case,
        links to the ancestral nodes are generated.
        :param pair: Pair as returned by the pair list functions
        :param level: Taxonomic level
        :param weight: If false, merges edges with different weights
        :return:
        """
        try:
            with self._driver.session() as session:
                agglom_1 = session.write_transaction(self._create_agglom)
                session.write_transaction(self._taxonomy, agglom_1, pair.nodes[0], level)
                session.write_transaction(self._rewire_edges, agglom_1, pair, weight)
                session.write_transaction(self._delete_old_agglomerations, ([pair.nodes[1]] + [pair.nodes[5]]))
        except Exception:
            logger.error("Could not agglomerate a pair of matching edges. \n", exc_info=True)

    @staticmethod
    def _pair_list(tx, level, weight, network):
        """
        Returns a list of edge pairs, where the
        taxonomic levels at both ends match, and the name of
        the edges are different. If 'weight' is specified as True,
        only edges with identical weight are returned.
        :param tx: Neo4j transaction
        :param level: Taxonomic level
        :param weight: if True, specifies that edge weights should have identical signs.
        :return: List of transaction outputs
        """
        if weight:
            result = tx.run(("MATCH (a:Edge)-[]-(:Network {name: '" + network +
                             "'})-[]-(b:Edge) WHERE (a.name <> b.name) "
                             "AND (a.weight = b.weight) "
                             "WITH a, b "
                             "MATCH p=(a:Edge)--()--(x:" + level +
                             ")--()--(b:Edge)--()--(y:" + level +
                             ")--()--(a:Edge) "
                             "RETURN p LIMIT 1"))
        else:
            result = tx.run(("MATCH (a:Edge)--(:Network {name: '" + network +
                             "'})--(b:Edge) WHERE (a.name <> b.name) "
                             "WITH a, b "
                             "MATCH p=(a:Edge)--()--(x:" + level +
                             ")--()--(b:Edge)--()--(y:" + level +
                             ")--()--(a:Edge) "
                             "RETURN p LIMIT 1"))
        return result.data()

    @staticmethod
    def _tax_list(tx, level, network):
        """
        Returns a list of taxon pairs, where the
        taxonomic levels match.
        Since each taxon is merged based on edge pairs,
        this function also only includes taxa belonging to the
        network that is being merged.

        :param tx: Neo4j transaction
        :param level: Taxonomic level
        :param network: Network
        :return: List of transaction outcomes
        """
        result = tx.run(("MATCH p=(e:" +
                         level + ")--(m)--(:Edge)--(:Network {name: '" + network +
                         "'})--(:Edge)--(n)--(f: " + level +
                         ") WHERE (e.name = f.name) "
                         "AND (m.name <> n.name) RETURN p LIMIT 1"))
        return result.data()

    @staticmethod
    def _create_agglom(tx):
        """
        Creates a Taxon node and returns its id.
        :param tx: Neo4j transaction
        :return: UID of new node
        """
        uid = str(uuid4())
        # non alphanumeric chars break networkx
        tx.run("CREATE (a:Taxon) SET a.name = $id", id=uid)
        return uid

    @staticmethod
    def _chainlinks(tx, node, source1, source2):
        """
        Each agglomerated Taxon node is linked to the Taxon node
        it originated from. Uses the UID length to check whether the old node
        was also an agglomerated taxon node.
        :param tx: Neo4j transaction
        :param node: UID of agglom_taxon
        :param source1: Source node of agglom_taxon as value in pairlist dictionary
        :param source2: Source node of agglom_taxon as value in pairlist dictionary
        :return:
        """
        names = [source1.get('name'), source2.get('name')]
        for name in names:
            if len(name) == 36:
                hits = tx.run(("MATCH (:Taxon {name: '" + name +
                               "'})-[:AGGLOMERATED]->(g) RETURN g"))
                for hit in hits.data():
                    old_link = tx.run(("MATCH p=(a:Taxon)-->(b:Taxon) WHERE a.name = '" +
                                       node + "' AND b.name ='" + hit['g'].get('name') +
                                       "' RETURN p")).data()
                    if len(old_link) == 0:
                        tx.run(("MATCH (a:Taxon),(b:Taxon) WHERE a.name = '" +
                                node + "' AND b.name = '" + hit['g'].get('name') +
                                "' CREATE (a)-[r:AGGLOMERATED]->(b) RETURN type(r)"))
            else:
                tx.run(("MATCH (a:Taxon),(b:Taxon) WHERE a.name = '" +
                        node + "' AND b.name = '" + name +
                        "' CREATE (a)-[r:AGGLOMERATED]->(b) RETURN type(r)"))

    @staticmethod
    def _rewire_edges(tx, node, path, weight):
        """
        Each aglommerated Taxon node is linked to the Taxon node
        it originated from. If it was generated from an agglomerated Taxon node,
        that source node's relationships to Taxon nodes are copied to the new node.
        :param tx: Neo4j transaction
        :param node: UID of agglomerated taxon
        :param path: Path containing two nodes to be merged
        :param weight: If false, merges edges with different weights
        :return:
        """
        network = path.nodes[3]['name']
        old1 = tx.run(("MATCH p=(a)--(:Edge)--(:Network {name: '" + network +
                       "'}) WHERE a.name = '" +
                       path.nodes[1].get('name') + "' RETURN p")).data()
        old2 = tx.run(("MATCH p=(a)--(:Edge)--(:Network {name: '" + network +
                       "'}) WHERE a.name = '" +
                       path.nodes[5].get('name') + "' RETURN p")).data()
        old_links = list()
        for item in old1:
            old_links.append(item['p'].nodes[1].get('name'))
        for item in old2:
            old_links.append(item['p'].nodes[1].get('name'))
        tx.run(("MATCH p=(a)-[r:PARTICIPATES_IN]-(:Edge)--(:Network {name: '" + network +
                "'}) WHERE a.name = '" +
                path.nodes[1].get('name') + "' DELETE r"))
        tx.run(("MATCH p=(a)-[r:PARTICIPATES_IN]-(:Edge)--(:Network {name: '" + network +
                "'}) WHERE a.name = '" +
                path.nodes[5].get('name') + "' DELETE r"))
        old_links = list(set(old_links))  # issue with self loops causing deletion issues
        targets = list()
        if weight:
            weights = list()
        selfloops = list()
        for assoc in old_links:
            # first need to check if the old edges are to the same taxa.
            tx.run(("MATCH (a:Taxon),(b:Edge) WHERE a.name = '" +
                    node + "' AND b.name = '" + assoc +
                    "' CREATE (a)-[r:PARTICIPATES_IN]->(b) RETURN type(r)")).data()
        for assoc in old_links:
            target = tx.run(("MATCH (a:Taxon)--(b:Edge)--(m) "
                             "WHERE a.name = '" + node +
                             "' AND b.name = '" + assoc +
                             "' AND NOT m:Network RETURN m")).data()
            if len(target) == 0:
                 # this can happen when the target is a loop between
                 # source1 and source 2
                 target = tx.run(("MATCH (m:Taxon)--(b:Edge)--(:Network {name: '" + network +
                                  "'}) WHERE m.name = '" + node +
                                  "' AND b.name = '" + assoc +
                                  "' RETURN m")).data()
                 tx.run(("MATCH (m:Taxon), (b:Edge) "
                         "WHERE m.name = '" + node +
                         "' AND b.name = '" + assoc +
                         "' CREATE (m)-[r:PARTICIPATES_IN]->(b) RETURN type(r)"))
            targets.append(target[0]['m'].get('name'))
            if weight:
                edge_weight = tx.run(("MATCH (a:Taxon)--(b:Edge)-[r]-(:Network {name: '" + network +
                                      "'}) WHERE a.name = '" + node +
                                      "' AND b.name = '" + assoc +
                                      "' RETURN b.weight")).data()
                weights.append(edge_weight[0]['b.weight'])
        while len(targets) > 1:
            item = targets[0]
            # write function for finding edges that have both matching
            # targets and matching weights, then merge them
            indices = [i for i, e in enumerate(targets) if e == item]
            if len(indices) > 1:
                matches = list()
                for i in range(1, len(indices)):
                    if weight:
                        if weights[indices[0]] == weights[indices[i]]:
                            matches.append(indices[i])
                    else:
                        matches.append(indices[i])
                if len(matches) == 0:
                    # this happens if there are matching targets, but not matching weights
                    del old_links[0]
                    del targets[0]
                    if weight:
                        del weights[0]
                else:
                    # pick one edge to keep
                    edge = tx.run(("MATCH (a:Edge)--(:Network {name: '" + network +
                                   "'}) WITH a "
                                   "MATCH p=(:Taxon {name: '" + node +
                                   "'})--(a)--(:Taxon {name: '" + item +
                                   "'}) RETURN a.name LIMIT 1")).data()
                    tx.run(("MATCH (a:Edge)--(:Network {name: '" + network +
                            "'}) WITH a "
                            "MATCH p=(:Taxon {name: '" + node +
                            "'})--(a)--(:Taxon {name: '" + item +
                            "'}) WHERE (a.name <> '" + edge[0]['a.name'] +
                            "') DETACH DELETE a"))
                    del old_links[0]
                    del targets[0]
                    if weight:
                        del weights[0]
            else:
                # if the weights do not match, the edge is not changed,
                # but the edge is removed from old_links, weights and targets
                del old_links[0]
                del targets[0]
                if weight:
                    del weights[0]

    @staticmethod
    def _taxonomy(tx, node, tax, level):
        """
        Adds appropriate taxonomic relationships to taxonomic nodes.
        Generally, if this function returns an error because the 'tree' query
        came up empty, this means the phylogenetic tree was discontinous.
        :param tx: Neo4j transaction
        :param node: Taxon name
        :param tax: Dictionary of taxonomic assignments
        :param level: Level to which taxonomy should be specified
        :return:
        """
        tax_list = ['Species', 'Genus', 'Family', 'Order', 'Class', 'Phylum', 'Kingdom']
        level_id = tax_list.index(level)
        # it is possible that the taxonomy has not been assigned at the specified level
        # in this case, the pattern finds the kingdom
        # the code below searches for lower taxonomic levels
        query_list = ["MATCH p=(:Species {name: '" + tax.get('name') +
                      "'})-->(:Genus)-->(:Family)-->(:Order)-->(:Class)-->(:Phylum)-->(:Kingdom) RETURN p",
                      "MATCH p=(:Genus {name: '" + tax.get('name') +
                      "'})-->(:Family)-->(:Order)-->(:Class)-->(:Phylum)-->(:Kingdom) RETURN p",
                      "MATCH p=(:Family {name: '" + tax.get('name') +
                      "'})-->(:Order)-->(:Class)-->(:Phylum)-->(:Kingdom) RETURN p",
                      "MATCH p=(:Order {name: '" + tax.get('name') +
                      "'})-->(:Class)-->(:Phylum)-->(:Kingdom) RETURN p",
                      "MATCH p=(:Class {name: '" + tax.get('name') +
                      "'})-->(:Phylum)-->(:Kingdom) RETURN p",
                      "MATCH p=(:Phylum {name: '" + tax.get('name') +
                      "'})-->(:Kingdom) RETURN p",
                      "MATCH p=(:Kingdom {name: '" + tax.get('name') + "'}) RETURN p"]
        query = query_list[level_id]
        tree = tx.run(query).data()[0]['p']
        for i in range(7-level_id):
            tx.run(("MATCH (a:Taxon),(b:" + tax_list[i+level_id] + ") "
                    "WHERE a.name = '" + node + "' AND b.name = '" +
                    tree.nodes[i].get('name') + "' CREATE (a)-[r:MEMBER_OF]->(b) RETURN type(r)"))

    @staticmethod
    def _get_partners(tx, edges):
        """
        Gets edge partners for all edges in dict.

        :param tx: Neo4j transaction
        :param edges: Dictionary with edge uuid and old + new network names
        :param source_network: Name of original network
        :param target_network: Name of network to connect copy to
        :return:
        """
        query = "WITH $batch as batch " \
                "UNWIND batch as record " \
                "MATCH (a)-[:PARTICIPATES_IN]-(n:Edge {name: record.name})-" \
                "[:PARTICIPATES_IN]-(b) WHERE (a.name <> b.name)" \
                " RETURN a,b,n"
        edge_partners = tx.run(query, batch=edges).data()
        # also return selfloops
        selfloop = []
        query = "WITH $batch as batch " \
                "UNWIND batch as record " \
                "MATCH (a)-[r:PARTICIPATES_IN]-(n:Edge {name: record.name})" \
                "-[q:PARTICIPATES_IN]-(a) " \
                "RETURN a, n"
        selfloop = tx.run(query, batch=edges).data()
        query = "WITH $batch as batch " \
                "UNWIND batch as record " \
                "MATCH ()-[r:PARTICIPATES_IN]-(n:Edge {name: record.name}) " \
                "WITH n, count(r) as rels " \
                "WHERE rels = 1 " \
                "RETURN n,rels"
        singles = tx.run(query, batch=edges).data()
        if len(singles) > 0:
            logger.error("Detected edge with only 1 interaction partner!", exc_info=True)
            for edge in singles:
                print(edge['a']['name'])
        edge_dict = dict()
        for edge in edge_partners:
            edge_dict[edge['n']['name']] = [edge['a']['name'], edge['b']['name']]
        for edge in selfloop:
            edge_dict[edge['n']['name']] = [edge['a']['name'], edge['a']['name']]
        return edge_dict

    @staticmethod
    def _copy_edges(tx, edges):
        """
        Takes a single edge and copies it while adding a connection to the new network.

        :param tx: Neo4j transaction
        :param edges: Dictionary with edge uuid, edge partners and network names
        :return:
        """
        query = "WITH $batch as batch " \
                "UNWIND batch as record " \
                "CREATE (a: Edge {name: record.uid, weight: record.weight}) " \
                "RETURN a"
        tx.run(query, batch=edges).data()
        query = "WITH $batch as batch " \
                "UNWIND batch as record " \
                "MATCH (a: Edge {name: record.uid}), " \
                "(b:Network {name: record.new}) " \
                "MERGE (a)-[r:PART_OF]->(b) " \
                "RETURN a"
        tx.run(query, batch=edges).data()
        query = "WITH $batch as batch " \
                "UNWIND batch as record " \
                "MATCH (a: Edge {name: record.uid}), " \
                "(b:Network {name: record.new}) " \
                "MERGE (a)-[r:PART_OF]->(b) " \
                "RETURN a"
        tx.run(query, batch=edges).data()
        query = "WITH $batch as batch " \
                "UNWIND batch as record " \
                "MATCH (a: Edge {name: record.uid}), " \
                "(b:Taxon {name: record.a}) " \
                "MERGE (a)-[r:PARTICIPATES_IN]->(b) " \
                "RETURN a"
        tx.run(query, batch=edges).data()
        query = "WITH $batch as batch " \
                "UNWIND batch as record " \
                "MATCH (a: Edge {name: record.uid}), " \
                "(b:Taxon {name: record.b}) " \
                "MERGE (a)-[r:PARTICIPATES_IN]->(b) " \
                "RETURN a"
        tx.run(query, batch=edges).data()

    @staticmethod
    def _create_edge(tx, agglom_1, agglom_2, network, edge_sign=None):
        """
        Creates new edges between agglomerated nodes, with
        the appropriate weight and Network node connections.
        :param tx: Neo4j transaction
        :param agglom_1: Source taxon UID
        :param agglom_2: Source taxon UID
        :param network: Network containing pair_list edges
        :param edge_sign: sign of edge
        :return:
        """
        uid = str(uuid4())
        # non alphanumeric chars break networkx
        tx.run("CREATE (a:Edge {name: $id}) RETURN a",
               id=uid)
        tx.run(("MATCH (a:Edge),(b:Taxon) "
                "WHERE a.name = '" + uid + "' AND b.name = '" +
                agglom_1 + "' CREATE (a)-[r:PARTICIPATES_IN]->(b) RETURN type(r)"))
        tx.run(("MATCH (a:Edge),(b:Taxon) "
                "WHERE a.name = '" + uid + "' AND b.name = '" +
                agglom_2 + "' CREATE (a)-[r:PARTICIPATES_IN]->(b) RETURN type(r)"))
        if edge_sign:
            tx.run(("MATCH (a:Edge),(b:Network) "
                    "WHERE a.name = '" + uid + "' AND b.name = '" +
                    network + "' CREATE (a)-[r:PART_OF]->(b) "
                              "SET a.weight = '" + str(edge_sign) + "' RETURN type(r)"))
        else:
            tx.run(("MATCH (a:Edge),(b:Network) "
                    "WHERE a.name = '" + uid + "' AND b.name = '" +
                    network + "' CREATE (a)-[r:PART_OF]->(b)"
                              " RETURN type(r)"))

    @staticmethod
    def _get_network(tx, nodes):
        """
        When a new edge is generated to replace two old ones,
        all Network nodes those were connected to are returned by this function.
        :param tx: Neo4j transaction
        :param nodes: List of edge names
        :return: List of network names
        """
        networks = list()
        for node in nodes:
            all_networks = tx.run("MATCH (:Edge {name: '" + node.get('name') +
                                  "'})-->(n:Network) RETURN n").data()
            for item in all_networks:
                networks.append(item['n'].get('name'))
        networks = list(set(networks))
        return networks

    @staticmethod
    def _delete_old_edges(tx, edges):
        """
        Deletes specific edges and their relationships.
        :param tx: Neo4j transaction
        :param edges: List of edge names
        :return:
        """
        for node in edges:
            tx.run(("MATCH (n:Edge {name: '" + node.get('name') + "'}) DETACH DELETE n"))

    @staticmethod
    def _delete_old_agglomerations(tx, nodes):
        """
        Deletes old agglomerated Taxon nodes.
        :param tx: Neo4j transaction
        :param nodes: List of edge names
        :return:
        """
        for node in nodes:
            result = tx.run(("MATCH (n:Taxon {name: '" + node.get('name') + "'}) RETURN n")).data()
            if len(result) > 0:
                if len(result[0]['n']['name']) == 36:
                    tx.run(("MATCH (n:Taxon {name: '" + node.get('name') + "'}) DETACH DELETE n"))

    @staticmethod
    def _hypergeom_population(tx, taxon, categ):
        """
        Returns 4 numbers:
        The number of samples in the database that is linked to the specified type,
        the number of samples in the database that is linked to a success,
        and the same values for the number of samples linked to the taxon.
        Only presence / absence is tested for, not differential abundance.
        :param tx: Neo4j transaction
        :param taxon: Taxon name
        :param categ: List containing metadata node type and categorical value representing success
        :return: List of population values necessary for hypergeometric test
        """
        type_val = categ[0]
        success = categ[1]
        hypergeom_vals = dict()
        query = "MATCH (n:Specimen)-->(:Property {name: '" + type_val + \
                "'}) RETURN n"
        total_samples = tx.run(query).data()
        hypergeom_vals['total_pop'] = _get_unique(total_samples, 'n', 'num')
        query = "MATCH (n:Specimen)-[r {value: '" + success + \
                "'}]->(:Property {name: '" + type_val + \
                "'}) RETURN n"
        total_samples = tx.run(query).data()
        hypergeom_vals['success_pop'] = _get_unique(total_samples, 'n', 'num')
        query = "MATCH (:Taxon {name: '" + taxon +\
                "'})-->(n:Specimen)-->(:Property {name: '" + type_val + \
                "'}) RETURN n"
        total_samples = tx.run(query).data()
        hypergeom_vals['total_taxon'] = _get_unique(total_samples, 'n', 'num')
        query = "MATCH (:Taxon {name: '" + taxon +\
                "'})-->(n:Specimen)-[r {value: '" + success + \
                "'}]->(:Property {name: '" + type_val + \
                "'}) RETURN n"
        total_samples = tx.run(query).data()
        hypergeom_vals['success_taxon'] = _get_unique(total_samples, 'n', 'num')
        return hypergeom_vals

    @staticmethod
    def _spearman_test(tx, taxon, type_val):
        """
        Returns p-value of Spearman correlation.
        :param tx: Neo4j transaction
        :param taxon: Taxon name
        :param type_val: Metadata node type
        :return: Spearman correlation and p-value
        """
        # get vector of sample values
        sample_values = list()
        sample_names = list()
        taxon_values = list()
        query = "MATCH (n:Specimen)-->(:Property {name: '" + type_val + \
                "'}) RETURN n"
        samples = _get_unique(tx.run(query).data(), 'n')
        for item in samples:
            query = "MATCH (:Specimen {name: '" + item + \
                    "'})-[r]->(n:Property {name: '" + type_val + \
                    "'}) RETURN r"
            sample_value = tx.run(query).data()[0]['r']['value']
            try:
                sample_value = float(sample_value)
            except ValueError:
                pass
            if type(sample_value) == float:
                sample_values.append(sample_value)
                sample_names.append(item)
        for sample in sample_names:
            query = "MATCH (:Specimen {name: '" + sample + \
                    "'})<-[r:LOCATED_IN]-(:Taxon {name: '" + taxon + \
                    "'}) RETURN r"
            counts = tx.run(query).data()
            if len(counts) == 0:
                count = 0
            else:
                count = float(counts[0]['r'].get('count'))
            taxon_values.append(count)
        result = spearmanr(taxon_values, sample_values)
        return result

    @staticmethod
    def _shortcut_categorical(tx, taxon, categ, prob):
        """
        Creates relationship between categorical variable and taxon.
        :param tx: Neo4j transaction
        :param taxon: Taxon name
        :param categ: List containing metadata node type and categorical value representing success
        :param prob: Outcome of hypergeometric test
        :return:
        """
        name = str(np.round(prob, 3))
        tx.run(("MATCH (a:Taxon {name: '" + taxon +
                "'}), (b:Property {name: '" + categ[0] +
                "'}) MERGE (a)-[r:HYPERGEOM]->(b)" +
                " SET r.value = " + name +
                " SET r.name = '" + categ[1] +
                "' RETURN type(r)"))

    @staticmethod
    def _shortcut_continuous(tx, taxon, var_dict):
        """
        Creates relationship between categorical variable and taxon.
        :param tx: Neo4j transaction
        :param taxon: Taxon name
        :param var_dict: Dict with variable and value
        :return:
        """
        var_id = list(var_dict.keys())[0]
        name = str(np.round(var_dict[var_id], 3))
        tx.run(("MATCH (a:Taxon {name: '" + taxon +
                "'}), (b:Property {name: '" + var_id +
                "'}) MERGE (a)-[r:HYPERGEOM]->(b)" +
                " SET r.value = " + name +
                " RETURN type(r)"))
