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
    if inputs['agglom']:
        tax_list = ['Species', 'Genus', 'Family', 'Order', 'Class', 'Phylum', 'Kingdom']
        level_id = tax_list.index(inputs['agglom'].capitalize())
        if inputs['weight']:
            mode = inputs['weight']
        else:
            mode = 'Ignore weight'
        for level in range(0, level_id + 1):
            # pub.sendMessage('update', msg="Agglomerating edges...")
            logger.info("Agglomerating edges...")
            driver.agglomerate_network(level=tax_list[level], mode=mode)
    if inputs['variable']:
        logger.info("Associating samples...  ")
        # sys.stdout.write("Associating samples...")
        if inputs['variable'][0] == 'all':
            properties = set([x[y] for x in driver.custom_query("MATCH (n:Property) RETURN n.type") for y in x])
            for prop in properties:
                driver.associate_samples(label=prop)
        else:
            for var in inputs['variable']:
                driver.associate_samples(label=var)
    logger.info('Completed metastats operations!  ')


class MetastatsDriver(ParentDriver):
    """
    Initializes a driver for accessing the Neo4j database.
    This driver extracts nodes and edges from the database that are required
    for the operations defined in the metastats module.
    """
    def agglomerate_network(self, level=None, mode='weight'):
        """
        Agglomerates to specified taxonomic level, or, if no level is specified,
        over all levels. Edges are agglomerated based on similarity
        at the specified taxonomic level. If the mode is set to 'weight',
        edges are only agglomerated if their weight matches.
        The stop condition is the length of the pair list;
        as soon as no pair meets the qualification, agglomeration is terminated.
        By default, agglomeration is done separately
        per network in the database.
        :param level: Taxonomic level matching taxonomic assignments in Neo4j database
        :param mode: if 'weight', takes edge weight into account
        :return:
        """
        try:
            stop_condition = False
            while not stop_condition:
                # limit is necessary to prevent excessively long queries
                pairs = self.get_pairlist(level=level, mode=mode)
                if len(pairs) > 0:
                    pair = pairs[0]
                    self.agglomerate_pair(pair, level=level, mode=mode)
                else:
                    # if no pairs are found, they may be with unassigned taxa
                    # the unassigned_pairlist function looks for those
                    pairs = self.get_unassigned_pairlist(level=level, mode=mode)
                    if len(pairs) > 0:
                        pair = pairs[0]
                        self.agglomerate_pair(pair, level=level, mode=mode)
                    else:
                        stop_condition = True
            stop_condition = False
            while not stop_condition:
                # after agglomerating associations
                # taxa with same taxonomic assignments should be merged
                # this rewires the network
                pairs = self.get_taxlist(level=level)
                if len(pairs) > 0:
                    pair = pairs[0]
                    self.agglomerate_taxa(pair, level=level)
                else:
                    # if no pairs are found, they may be with unassigned taxa
                    # the unassigned_pairlist function looks for those
                    pairs = self.get_unassigned_taxlist(level=level)
                    if len(pairs) > 0:
                        pair = pairs[0]
                        self.agglomerate_taxa(pair, level=level)
                    else:
                        stop_condition = True
        except Exception:
            logger.error("Could not agglomerate edges to higher taxonomic levels. \n", exc_info=True)

    def get_pairlist(self, level, mode):
        """
        Starts a new transaction for every pair list request.
        A pair is defined as two edges linked to taxonomic nodes
        that have the same taxonomic assignment at the specified level,
        e.g. Nitrobacter-association-Nitrosomonas.
        :param level: Taxonomic level to identify a pair.
        :param mode: if 'weight', specifies that edges should have identical weights.
        :return: List containing results of Neo4j transaction
        """
        pairs = None
        try:
            with self._driver.session() as session:
                pairs = session.read_transaction(self._pair_list, level, mode)
        except Exception:
            logger.error("Could not obtain list of matching edges. \n", exc_info=True)
        return pairs

    def get_unassigned_pairlist(self, level, mode):
        """
        Starts a new transaction for every pair list request.
        This function gets pairs that have missing taxonomic assignments at the specified level.
        :param level: Taxonomic level to identify a pair.
        :param mode: if 'weight', specifies that edges should have identical weights.
        :return: List containing results of Neo4j transaction
        """
        try:
            with self._driver.session() as session:
                pairs = session.read_transaction(self._unassigned_pairlist, level, mode)
                return pairs
        except Exception:
            logger.error("Could not obtain list of matching edges. \n", exc_info=True)

    def agglomerate_pair(self, pair, level, mode):
        """
        For one pair, as returned by get_pairlist,
        this function creates new agglomerated nodes,
        deletes old agglomerated nodes, and chains taxonomic nodes
        to the new agglomerated nodes. Morever, the two old edges
        are deleted and replaced by a new association.
        :param pair: List containing transaction results of query for pair
        :param level: Taxonomic level to identify a pair.
        :param mode: if 'weight', specifies that edges should have identical weights.
        :return:
        """
        try:
            with self._driver.session() as session:
                agglom_1 = session.write_transaction(self._create_agglom)
                agglom_2 = session.write_transaction(self._create_agglom)
                session.write_transaction(self._chainlinks, agglom_1, pair['p'].nodes[1], pair['r'].nodes[1])
                session.write_transaction(self._chainlinks, agglom_2, pair['p'].nodes[3], pair['r'].nodes[3])
                session.write_transaction(self._taxonomy, agglom_1, pair['p'].nodes[0], pair['p'].nodes[1], level)
                session.write_transaction(self._taxonomy, agglom_2, pair['r'].nodes[4], pair['r'].nodes[3], level)
                networks = session.read_transaction(self._get_network, [pair['p'].nodes[2], pair['r'].nodes[2]])
                weight = session.read_transaction(self._get_weight, [pair['p'].nodes[2], pair['r'].nodes[2]])
                session.write_transaction(self._create_association, agglom_1, agglom_2, networks, weight, mode)
            with self._driver.session() as session:
                session.write_transaction(self._delete_old_edges, [pair['p'].nodes[2], pair['r'].nodes[2]])
                session.write_transaction(self._delete_old_agglomerations, (pair['p'].nodes + pair['r'].nodes))
        except Exception:
            logger.error("Could not agglomerate a pair of matching edges. \n", exc_info=True)

    def associate_samples(self, type, null_input=None):
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
        :param type: Type of property (e.g. pH) to query.
        :param null_input: If missing values are not specified as NA, specify the NA input here.
        :return:
        """
        with self._driver.session() as session:
            type_nodes = session.read_transaction(self._query, "MATCH (n:Property) WHERE n.type = '" +
                                                  type + "' RETURN n.name")
        properties = list(set().union(*(d.values() for d in type_nodes)))
        try:
            with self._driver.session() as session:
                tax_nodes = session.read_transaction(self._query, "MATCH (n)--(:Edge) WHERE n:Taxon RETURN n")
                tax_nodes = _get_unique(tax_nodes, 'n')
            for node in tax_nodes:
                self.associate_taxon(mode='Taxon', taxon=node, null_input=null_input, properties=properties)
            with self._driver.session() as session:
                tax_nodes = session.read_transaction(self._query, "MATCH (n)--(:Edge) WHERE n:Agglom_Taxon RETURN n")
                tax_nodes = _get_unique(tax_nodes, 'n')
            for node in tax_nodes:
                self.associate_taxon(taxon=node, mode='Agglom_Taxon', null_input=null_input, properties=properties)
        except Exception:
            logger.error("Could not associate sample variables to taxa. \n", exc_info=True)

    def associate_taxon(self, taxon, mode, null_input, properties):
        """
        Tests whether specific sample properties can be associated to a taxon.
        :param taxon: Name of a taxon.
        :param mode: Can be 'Taxon' or 'Agglom_Taxon', specifies which label of taxonomic node.
        :param null_input: If missing values are not specified as NA, specify the NA input here.
        :param properties: List specifying types of properties to query.
        :return:
        """
        try:
            conts = list()
            categs = list()
            with self._driver.session() as session:
                if mode == 'Taxon':
                    query = "WITH " + str(properties) + \
                            " as names MATCH (:Taxon {name: '" + taxon + \
                            "'})-->(:Sample)-->(n:Property) WHERE n.name in names RETURN n"
                if mode == 'Agglom_Taxon':
                    query = "WITH " + str(properties) + \
                            " as names MATCH (:Agglom_Taxon {name: '" + taxon + \
                            "'})-[:GENERATED_FROM]-(:Taxon)--" \
                            "(:Sample)-->(n:Property) WHERE n.name in names RETURN n"
                sample_properties = session.read_transaction(self._query, query)
                for item in sample_properties:
                    value = item['n'].get('name')
                    if value == null_input:
                        break
                    # try to convert value to float; if successful, adds type to continous vars
                    try:
                        value = float(value)
                    except ValueError:
                        pass
                    if type(value) == float:
                        conts.append(item['n'].get('type'))
                    else:
                        categs.append([item['n'].get('type'), item['n'].get('name')])
            conts = set(conts)
            categs = set(tuple(categ) for categ in categs)
            for categ_val in categs:
                with self._driver.session() as session:
                    hypergeom_vals = session.read_transaction(self._hypergeom_population, taxon, categ_val, mode)
                    prob = hypergeom.cdf(hypergeom_vals['success_taxon'], hypergeom_vals['total_pop'],
                                         hypergeom_vals['success_pop'], hypergeom_vals['total_taxon'])
                    if prob < 0.05:
                        session.write_transaction(self._shortcut_categorical, taxon, categ_val, mode, prob)
            for cont_val in conts:
                with self._driver.session() as session:
                    spearman_result = session.read_transaction(self._spearman_test,
                                                               taxon, cont_val, mode)
                    if spearman_result.pvalue < 0.05:
                        var_dict = {cont_val: spearman_result.correlation}
                        session.write_transaction(self._shortcut_continuous, taxon, var_dict, mode)
        except Exception:
            logger.error("Could not associate a specific taxon to sample variables. \n", exc_info=True)

    def get_taxlist(self, level):
        """
        Starts a new transaction for every tax list request.
        A tax list is a list containing two edges linked to identical taxa.
        :param level: Taxonomic level.
        :return: List of transaction outcomes
        """
        pairs = None
        try:
            with self._driver.session() as session:
                pairs = session.read_transaction(self._tax_list, level)
        except Exception:
            logger.error("Could not obtain list of matching taxa. \n", exc_info=True)
        return pairs

    def get_unassigned_taxlist(self, level):
        """
        Starts a new transaction for every tax list request.
        This tax list contains edges with nodes that do not have
        an assignment at the specified level.
        :param level: Taxonomic level.
        :return: List of transaction outcomes
        """
        pairs = None
        try:
            with self._driver.session() as session:
                pairs = session.read_transaction(self._unassigned_tax_list, level)
        except Exception:
            logger.error("Could not obtain list of matching taxa. \n", exc_info=True)
        return pairs

    def agglomerate_taxa(self, pair, level):
        """
        For one pair, as returned by get_taxlist,
        this function merges nodes with similar taxonomy
        but different edges together.
        Old nodes are linked to the new agglomerated node,
        except for Agglom_Taxon; in that case,
        links to the ancestral nodes are generated.
        :param pair: Pair as returned by the pair list functions
        :param level: Taxonomic level
        :return:
        """
        try:
            with self._driver.session() as session:
                agglom_1 = session.write_transaction(self._create_agglom)
                session.write_transaction(self._chainlinks, agglom_1, pair['p'].nodes[1], pair['r'].nodes[1])
                session.write_transaction(self._taxonomy, agglom_1, pair['p'].nodes[0], pair['p'].nodes[1], level)
                session.write_transaction(self._rewire_edges, agglom_1, pair['p'], pair['r'])
                session.write_transaction(self._delete_old_agglomerations, ([pair['p'].nodes[1]] + [pair['r'].nodes[1]]))
        except Exception:
            logger.error("Could not agglomerate a pair of matching edges. \n", exc_info=True)

    def export_fasta(self, fp, name):
        """
        This function exports a FASTA file compatible with other tools,
        e.g. PICRUSt2.
        The advantage of using this FASTA file is that it
        only contains taxa present in the database.
        Hence, tools like PICRUSt2 will run much faster.
        While massoc cannot directly run PICRUSt2, the below command
        is an example of how you could generate a PICRUSt2 table to provide to massoc.
        You don't need to run the full PICRUSt2 pipeline because
        massoc will not use the predicted function abundances.
        For the cheese demo, you could run PICRUSt2 as follows:
        place seqs.py -s cheese.fasta -o cheese.tre -p 1
        :param fp: Output filepath for storing intermediate files.
        :param name: List of names for files in database.
        :return:
        """
        # first run the system_call_check place_seqs_cmd
        # many of the commands are default
        # we can extract a fasta of sequences from the database
        with self._driver.session() as session:
            study_fasta = session.read_transaction(self._get_fasta)
        file = open(fp + "//" + ''.join(name) + ".fasta", "w")
        file.write(study_fasta)
        file.close()
        # use default reference files


    @staticmethod
    def _query(tx, query):
        """
        Processes custom queries.
        :param tx: Neo4j transaction
        :param query: String specifying Cypher query
        :return:
        """
        results = tx.run(query).data()
        return results

    @staticmethod
    def _pair_list(tx, level, mode):
        """
        Returns a list of association pairs, where the
        taxonomic levels at both ends match, and the name of
        the edges are different. If 'weight' is specified as mode,
        only edges with identical weight are returned.
        :param tx: Neo4j transaction
        :param level: Taxonomic level
        :param mode: if 'weight', searches for edges with matching weights
        :return: List of transaction outputs
        """
        if mode == 'weight':
            result = tx.run(("MATCH p=(e:" +
                             level + ")<--()<--(a:Edge)-->()-->(g:" +
                             level + ") MATCH r=(h:" + level +
                             ")<--()<--(b:Edge)-->()-->(f:" +
                             level +
                             ") MATCH (x)--(:Network)--(y)"
                             " WHERE (a.name <> b.name) AND (a.weight = b.weight) AND "
                             "(e.name = h.name) AND (g.name = f.name) "
                             "AND (x.name = a.name) AND (y.name = b.name) "
                             "RETURN p,r LIMIT 1"))
        else:
            result = tx.run(("MATCH p=(e:" + level +
                             ")<--()<--(a:Edge)-->()-->(g:" + level +
                             ") MATCH r=(h:" + level +
                             ")<--()<--(b:Edge)-->()-->(f:" + level +
                             ") MATCH (x)--(:Network)--(y)"
                             " WHERE (a.name <> b.name) AND "
                             "(e.name = h.name) AND (g.name = f.name) "
                             "AND (x.name = a.name) AND (y.name = b.name)"
                             "RETURN p,r LIMIT 1"))
        return result.data()

    @staticmethod
    def _tax_list(tx, level):
        """
        Returns a list of taxon pairs, where the
        taxonomic levels match.
        :param tx: Neo4j transaction
        :param level: Taxonomic level
        :return: List of transaction outcomes
        """
        result = tx.run(("MATCH p=(e:" +
                         level + ")--(m)--(:Edge) MATCH r=(h:" + level +
                         ")<--(n)--(:Edge) WHERE (m.name <> n.name) "
                         "AND (e.name = h.name) RETURN p,r LIMIT 1"))
        return result.data()

    @staticmethod
    def _unassigned_tax_list(tx, level):
        """
        Returns a list of taxon pairs, where the
        taxonomic levels are lacking for both taxa.
        :param tx: Neo4j transaction
        :param level: Taxonomic level
        :return: List of transaction outcomes
        """
        levels = ['Species', 'Genus', 'Family', 'Order', 'Class', 'Phylum', 'Kingdom']
        current = levels.index(level)
        out = list()
        for j in range(current, 6):
            result = tx.run(("MATCH p=(e:" + levels[j+1] + ")--(m)--(:Edge)--(k)--(:Kingdom) "
                             "MATCH r=(h:" + levels[j+1] + ")<--(n)--(:Edge)--()--(:Kingdom) "
                             "WHERE (m.name <> n.name) AND (k.name <> n.name) "
                             "AND (e.name = h.name) AND NOT (n)--(:" + levels[j] +
                             ") AND NOT (m)--(:" + levels[j] +
                             ")RETURN p,r")).data()
            if len(result) > 0:
                pair = result[0]
                out.append(pair)
                break
        return out

    @staticmethod
    def _unassigned_pairlist(tx, level, mode):
        """
        Returns a list of association pairs, where one node
        has not been assigned the specified taxonomic level.
        Once these results are returned, the pair is checked for
        matching taxonomy at higher taxonomic levels;
        if they match there, the pair is returned.
        :param tx: Neo4j transaction
        :param level: Taxonomic level
        :param mode: if 'weight', searches for edges with matching weights
        :return: List of transaction outcomes
        """
        levels = ['Species', 'Genus', 'Family', 'Order', 'Class', 'Phylum', 'Kingdom']
        current = levels.index(level)
        out = list()
        for j in range(current, 6):
            if mode == 'weight':
                result = tx.run(("MATCH p=(e:" + levels[j + 1] +
                                 ")<--(m)<--(a:Edge)-->()--(f:" + level +
                                 ") MATCH r=(h:" + levels[j + 1] +
                                 ")<--(n)<--(b:Edge)-->()--(g:" + level +
                                 ") MATCH (x)--(:Network)--(y)"
                                 " WHERE (a.name <> b.name) AND (a.weight = b.weight) AND "
                                 "(e.name = h.name) AND (f.name = g.name) "
                                 "AND (x.name = a.name) AND (y.name = b.name) "
                                 "AND NOT (n)--(:" + level + ") AND NOT (m)--(:" + level +
                                 ") RETURN p,r LIMIT 1")).data()
            else:
                result = tx.run(("MATCH p=(e:" + levels[j + 1] +
                                 ")<--(m)<--(a:Edge)-->()--(f:" + level +
                                 ") MATCH r=(h:" + levels[j + 1] +
                                 ")<--(n)<--(b:Edge)-->()--(g:" + level +
                                 ") MATCH (x)--(:Network)--(y)"
                                 " WHERE (a.name <> b.name) AND "
                                 "(e.name = h.name) AND (f.name = g.name) "
                                 "AND (x.name = a.name) AND (y.name = b.name) "
                                 "AND NOT (n)--(:" + level + ") AND NOT (m)--(:" + level +
                                 ") RETURN p,r LIMIT 1")).data()
            if len(result) > 0:
                pair = result[0]
                out.append(pair)
                break
            # we can only find taxa that do not have current taxonomic level assigned
            # next step is to check for each pair if they match at a higher taxonomic level
            found = False
            i = 0
            out = list()
            if len(result) > 0:
                while not found and i < len(result):
                    pair = result[i]
                    i += 1
                    j = current + 1
                    match = False
                    while not match:
                        tax1 = pair['p'].nodes[3]
                        tax2 = pair['r'].nodes[3]
                        level1 = tx.run(("MATCH (m)--(n:" + levels[j] +
                                       ") WHERE (m.name = '" + tax1.get('name') + "') RETURN n")).data()
                        level2 = tx.run(("MATCH (m)--(n:" + levels[j] +
                                       ") WHERE (m.name = '" + tax2.get('name') + "') RETURN n")).data()
                        if len(level1) == 0 and len(level2) == 0:
                            j += 1
                        elif len(level1) == len(level2):
                            if level1[0]['n'].get('name') == level1[0]['n'].get('name'):
                                match = True
                                found = True
                                out.append(pair)
                        else:
                            match = True
        return out

    @staticmethod
    def _create_agglom(tx):
        """
        Creates an Agglom_Taxon node and returns its id.
        :param tx: Neo4j transaction
        :return: UID of new node
        """
        uid = str(uuid4())
        # non alphanumeric chars break networkx
        tx.run("CREATE (a:Agglom_Taxon) SET a.name = $id", id=uid)
        return uid

    @staticmethod
    def _chainlinks(tx, node, source1, source2):
        """
        Each Agglom_Taxon node is linked to the Taxon node
        it originated from. Uses the UID length to check whether the old node
        was also an agglom_taxon node.
        :param tx: Neo4j transaction
        :param node: UID of agglom_taxon
        :param source1: Source node of agglom_taxon as value in pairlist dictionary
        :param source2: Source node of agglom_taxon as value in pairlist dictionary
        :return:
        """
        names = [source1.get('name'), source2.get('name')]
        for name in names:
            if len(name) == 36:
                hits = tx.run(("MATCH (:Agglom_Taxon {name: '" + name +
                               "'})-[:GENERATED_FROM]->(g) RETURN g"))
                for hit in hits.data():
                    old_link = tx.run(("MATCH p=(a:Agglom_Taxon)-->(b:Taxon) WHERE a.name = '" +
                                       node + "' AND b.name ='" + hit['g'].get('name') +
                                       "' RETURN p")).data()
                    if len(old_link) == 0:
                        tx.run(("MATCH (a:Agglom_Taxon),(b:Taxon) WHERE a.name = '" +
                                node + "' AND b.name = '" + hit['g'].get('name') +
                                "' CREATE (a)-[r:GENERATED_FROM]->(b) RETURN type(r)"))
            else:
                old_link = tx.run(("MATCH p=(a:Agglom_Taxon)-->(b:Taxon) WHERE a.name = '" +
                                   node + "' AND b.name ='" + name +
                                   "' RETURN p")).data()
                if len(old_link) == 0:
                    tx.run(("MATCH (a:Agglom_Taxon),(b:Taxon) WHERE a.name = '" +
                            node + "' AND b.name = '" + name +
                            "' CREATE (a)-[r:GENERATED_FROM]->(b) RETURN type(r)"))

    @staticmethod
    def _rewire_edges(tx, node, source1, source2):
        """
        Each Agglom_Taxon node is linked to the Taxon node
        it originated from. If it was generated from an Agglom_Taxon node,
        that source node's relationships to Taxon nodes are copied to the new node.
        :param tx: Neo4j transaction
        :param node: UID of agglom_taxon
        :param source1: Source node of agglom_taxon as value in pairlist dictionary
        :param source2: Source node of agglom_taxon as value in pairlist dictionary
        :return:
        """
        old1 = tx.run(("MATCH p=(a)--(:Edge) WHERE a.name = '" +
                                    source1.nodes[1].get('name') + "' RETURN p")).data()
        old2 = tx.run(("MATCH p=(a)--(:Edge) WHERE a.name = '" +
                                    source2.nodes[1].get('name') + "' RETURN p")).data()
        old_links = list()
        for item in old1:
            old_links.append(item['p'].nodes[1].get('name'))
        for item in old2:
            old_links.append(item['p'].nodes[1].get('name'))

        tx.run(("MATCH p=(a)-[r:WITH_TAXON]-(:Edge) WHERE a.name = '" +
                source1.nodes[1].get('name') + "' DELETE r"))
        tx.run(("MATCH p=(a)-[r:WITH_TAXON]-(:Edge) WHERE a.name = '" +
                source2.nodes[1].get('name') + "' DELETE r"))
        old_links = list(set(old_links))  # issue with self loops causing deletion issues
        targets = list()
        weights = list()
        selfloops = list()
        for assoc in old_links:
            # first need to check if the old edges are to the same taxa.
            tx.run(("MATCH (a:Agglom_Taxon),(b:Edge) WHERE a.name = '" +
                    node + "' AND b.name = '" + assoc +
                    "' CREATE (a)-[r:WITH_TAXON]->(b) RETURN type(r)")).data()
        for assoc in old_links:
            target = tx.run(("MATCH (a:Agglom_Taxon)--(b:Edge)--(m) "
                             "WHERE a.name = '" + node +
                             "' AND b.name = '" + assoc +
                             "' AND NOT m:Network RETURN m")).data()
            if len(target) == 0:
                 # this can happen when the target is a loop between
                 # source1 and source 2
                 target = tx.run(("MATCH (m:Agglom_Taxon)--(b:Edge) "
                                               "WHERE m.name = '" + node +
                                               "' AND b.name = '" + assoc +
                                               "' RETURN m")).data()
                 tx.run(("MATCH (m:Agglom_Taxon), (b:Edge) "
                                          "WHERE m.name = '" + node +
                                          "' AND b.name = '" + assoc +
                                          "' CREATE (m)-[r:WITH_TAXON]->(b) RETURN type(r)"))
            weight = tx.run(("MATCH (a:Agglom_Taxon)--(b:Edge) "
                             "WHERE a.name = '" + node +
                             "' AND b.name = '" + assoc +
                             "' RETURN b.weight")).data()
            targets.append(target[0]['m'].get('name'))
            weights.append(weight[0]['b.weight'])
        while len(targets) > 1:
            item = targets[0]
            # write function for finding edges that have both matching
            # targets and matching weights, then merge them
            indices = [i for i, e in enumerate(targets) if e == item]
            if len(indices) > 1:
                matches = list()
                for i in range(1, len(indices)):
                    if weights[indices[0]] == weights[indices[i]]:
                        matches.append(indices[i])
                if len(matches) == 0:
                    # this happens if there are matching targets, but not matching weights
                    del old_links[0]
                    del targets[0]
                    del weights[0]
                for match in matches:
                    # if the weights of the edges with the same targets match
                    # the network links are added to indices[0]
                    # and the association of indices[1] is removed
                    networks_1 = tx.run(("MATCH (a:Edge {name: '" + old_links[indices[0]] +
                                         "'})--(m:Network) RETURN m")).data()
                    networks_2 = tx.run(("MATCH (a:Edge {name: '" + old_links[match] +
                                         "'})--(m:Network) RETURN m")).data()
                    all_networks = networks_1 + networks_2
                    netnames = list()
                    for network in all_networks:
                        netnames.append(network['m'].get('name'))
                    netnames = list(set(netnames))
                    # first delete all old network relationships of node 0
                    tx.run(("MATCH (a:Edge {name: '" + old_links[indices[0]] +
                            "'})-[r:IN_NETWORK]->(m:Network) DELETE r"))
                    # next delete matching association from database
                    tx.run(("MATCH (a:Edge {name: '" + old_links[match] +
                            "'}) DETACH DELETE a"))
                    # remove association from old_links, targets and weights
                    for network in netnames:
                        tx.run(("MATCH (a:Network),(b:Edge) WHERE a.name = '" +
                                network + "' AND b.name = '" + old_links[indices[0]] +
                                "' CREATE (a)<-[r:Edge]-(b) RETURN type(r)")).data()
                    del old_links[match]
                    del targets[match]
                    del weights[match]
            else:
                # if the weights do not match, the association is not changed,
                # but the association is removed from old_links, weights and targets
                del old_links[0]
                del targets[0]
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
        rel_list = ['IS_SPECIES', 'IS_GENUS', 'IS_FAMILY', 'IS_ORDER', 'IS_CLASS', 'IS_PHYLUM', 'IS_KINGDOM']
        level_id = tax_list.index(level)
        # it is possible that the taxonomy has not been assigned at the specified level
        # in this case, the pattern finds the kingdom
        # the code below searches for lower taxonomic levels
        if list(tax.labels)[0] != level:
            level_id = tax_list.index(list(tax.labels)[0])
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
            rel = rel_list[i+level_id]
            tx.run(("MATCH (a:Agglom_Taxon),(b:" + tax_list[i+level_id] + ") "
                    "WHERE a.name = '" + node + "' AND b.name = '" +
                    tree.nodes[i].get('name') + "' CREATE (a)-[r:" + rel + "]->(b) RETURN type(r)"))

    @staticmethod
    def _create_association(tx, agglom_1, agglom_2, networks, weight, mode):
        """
        Creates new edges between agglomerated nodes, with
        the appropriate weight and Network node connections.
        :param tx: Neo4j transaction
        :param agglom_1: Source agglom_taxon UID
        :param agglom_2: Source agglom_taxon UID
        :param networks: Networks containing pair_list edges
        :param weight: weight of node
        :param mode: if 'weight', generates weighted association
        :return:
        """
        uid = str(uuid4())
        # non alphanumeric chars break networkx
        if mode is 'weight':
            tx.run("CREATE (a:Edge {name: $id}) SET a.weight = $weight RETURN a",
                   id=uid, weight=str(weight))
        else:
            tx.run("CREATE (a:Edge {name: $id}) RETURN a",
                   id=uid)
        tx.run(("MATCH (a:Edge),(b:Agglom_Taxon) "
                "WHERE a.name = '" + uid + "' AND b.name = '" +
                agglom_1 + "' CREATE (a)-[r:WITH_TAXON]->(b) RETURN type(r)"))
        tx.run(("MATCH (a:Edge),(b:Agglom_Taxon) "
                "WHERE a.name = '" + uid + "' AND b.name = '" +
                agglom_2 + "' CREATE (a)-[r:WITH_TAXON]->(b) RETURN type(r)"))
        for node in networks:
            tx.run(("MATCH (a:Edge),(b:Network) "
                    "WHERE a.name = '" + uid + "' AND b.name = '" +
                    node + "' CREATE (a)-[r:IN_NETWORK]->(b) RETURN type(r)"))

    @staticmethod
    def _get_network(tx, nodes):
        """
        When a new association is generated to replace two old ones,
        all Network nodes those were connected to are returned by this function.
        :param tx: Neo4j transaction
        :param nodes: List of association names
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
    def _get_weight(tx, nodes):
        """
        Returns the weight of an Edge node.
        :param tx: Neo4j transaction
        :param nodes: List of association names
        :return: List of association weights
        """
        weight = tx.run("MATCH (n:Edge {name: '" + nodes[0].get('name') +
                        "'}) RETURN n").data()[0]['n'].get('weight')
        return weight

    @staticmethod
    def _delete_old_edges(tx, edges):
        """
        Deletes specific edges and their relationships.
        :param tx: Neo4j transaction
        :param edges: List of association names
        :return:
        """
        for node in edges:
            tx.run(("MATCH (n {name: '" + node.get('name') + "'}) DETACH DELETE n"))

    @staticmethod
    def _delete_old_agglomerations(tx, nodes):
        """
        Deletes old Agglom_Taxon nodes.
        :param tx: Neo4j transaction
        :param nodes: List of association names
        :return:
        """
        for node in nodes:
            result = tx.run(("MATCH (n:Agglom_Taxon {name: '" + node.get('name') + "'}) RETURN n")).data()
            if len(result) > 0:
                tx.run(("MATCH (n:Agglom_Taxon {name: '" + node.get('name') + "'}) DETACH DELETE n"))

    @staticmethod
    def _hypergeom_population(tx, taxon, categ, mode):
        """
        Returns 4 numbers:
        The number of samples in the database that is linked to the specified type,
        the number of samples in the database that is linked to a success,
        and the same values for the number of samples linked to the taxon.
        Only presence / absence is tested for, not differential abundance.
        :param tx: Neo4j transaction
        :param taxon: Taxon name
        :param categ: List containing metadata node type and categorical value representing success
        :param mode: Carries out hypergeometric test on 'Taxon' or 'Agglom_Taxon'
        :return: List of population values necessary for hypergeometric test
        """
        type_val = categ[0]
        success = categ[1]
        hypergeom_vals = dict()
        query = "MATCH (n:Sample)-->(:Property {type: '" + type_val + \
                "'}) RETURN n"
        total_samples = tx.run(query).data()
        hypergeom_vals['total_pop'] = _get_unique(total_samples, 'n', 'num')
        query = "MATCH (n:Sample)-->(:Property {type: '" + type_val + \
                "', name: '" + success + "'}) RETURN n"
        total_samples = tx.run(query).data()
        hypergeom_vals['success_pop'] = _get_unique(total_samples, 'n', 'num')
        if mode is 'Taxon':
            query = "MATCH (:Taxon {name: '" + taxon +\
                    "'})-->(n:Sample)-->(:Property {type: '" + type_val + \
                    "'}) RETURN n"
            total_samples = tx.run(query).data()
            hypergeom_vals['total_taxon'] = _get_unique(total_samples, 'n', 'num')
        if mode is 'Agglom_Taxon':
            query = "MATCH (:Agglom_Taxon {name: '" + taxon +\
                    "'})-[:GENERATED_FROM]-(:Taxon)--(n:Sample)-->" \
                    "(:Property {type: '" + type_val + \
                    "'}) RETURN n"
            total_samples = tx.run(query).data()
            hypergeom_vals['total_taxon'] = _get_unique(total_samples, 'n', 'num')
        if mode is 'Taxon':
            query = "MATCH (:Taxon {name: '" + taxon +\
                    "'})-->(n:Sample)-->(:Property {type: '" + type_val + \
                    "', name: '" + success + "'}) RETURN n"
            total_samples = tx.run(query).data()
            hypergeom_vals['success_taxon'] = _get_unique(total_samples, 'n', 'num')
        if mode is 'Agglom_Taxon':
            query = "MATCH (:Agglom_Taxon {name: '" + taxon +\
                    "'})-[:GENERATED_FROM]-(:Taxon)-->(n:Sample)-->" \
                    "(:Property {type: '" + type_val + \
                    "', name: '" + success + "'}) RETURN n"
            total_samples = tx.run(query).data()
            hypergeom_vals['success_taxon'] = _get_unique(total_samples, 'n', 'num')
        return hypergeom_vals

    @staticmethod
    def _spearman_test(tx, taxon, type_val, mode):
        """
        Returns p-value of Spearman correlation.
        :param tx: Neo4j transaction
        :param taxon: Taxon name
        :param type_val: Metadata node type
        :param mode: Carries out correlation on 'Taxon' or 'Agglom_Taxon'
        :return: Spearman correlation and p-value
        """
        # get vector of sample values
        sample_values = list()
        sample_names = list()
        taxon_values = list()
        query = "MATCH (n:Sample)-->(:Property {type: '" + type_val + \
                "'}) RETURN n"
        samples = _get_unique(tx.run(query).data(), 'n')
        for item in samples:
            query = "MATCH (:Sample {name: '" + item + \
                    "'})-->(n:Property {type: '" + type_val + \
                    "'}) RETURN n"
            sample_value = tx.run(query).data()[0]['n'].get('name')
            try:
                sample_value = float(sample_value)
            except ValueError:
                pass
            if type(sample_value) == float:
                sample_values.append(sample_value)
                sample_names.append(item)
        for sample in sample_names:
            if mode is 'Taxon':
                query = "MATCH (:Sample {name: '" + sample + \
                        "'})<-[r:FOUND_IN]-(:Taxon {name: '" + taxon + \
                        "'}) RETURN r"
                counts = tx.run(query).data()
                if len(counts) == 0:
                    count = 0
                else:
                    count = float(counts[0]['r'].get('count'))
            if mode is 'Agglom_Taxon':
                query = "MATCH (:Sample {name: '" + sample + \
                        "'})<-[r:FOUND_IN]-(:Taxon)-[:GENERATED_FROM]-" \
                        "(:Agglom_Taxon {name: '" + taxon + \
                        "'}) RETURN r"
                counts = tx.run(query).data()
                if len(counts) == 0:
                    count = 0
                else:
                    count = 0
                    for item in counts:
                        count += float(item['r'].get('count'))
            taxon_values.append(count)
        result = spearmanr(taxon_values, sample_values)
        return result

    @staticmethod
    def _shortcut_categorical(tx, taxon, categ, mode, prob):
        """
        Creates relationship between categorical variable and taxon.
        :param tx: Neo4j transaction
        :param taxon: Taxon name
        :param categ: List containing metadata node type and categorical value representing success
        :param mode: Carries out hypergeometric test on 'Taxon' or 'Agglom_Taxon'
        :param prob: Outcome of hypergeometric test
        :return:
        """
        hit = tx.run(("MATCH (a:Property {type: 'hypergeom_" + categ[0] +
                      "', name: '" + str(prob) + "'}) RETURN a")).data()
        if len(hit) == 0:
            tx.run(("CREATE (a:Property {type: 'hypergeom_" + categ[0] +
                    "', name: '" + str(prob) + "'}) RETURN a"))
        tx.run(("MATCH (a:" + mode +
                "),(b:Property) "
                "WHERE a.name = '" + taxon +
                "' AND b.name = '" + str(prob) +
                "' AND b.type = 'hypergeom_" + categ[0] +
                "' CREATE (a)-[r:HYPERGEOM]->(b) "
                "RETURN type(r)"))

    @staticmethod
    def _shortcut_continuous(tx, taxon, type_val, mode):
        """
        Creates relationship between categorical variable and taxon.
        :param tx: Neo4j transaction
        :param taxon: Taxon name
        :param type_val: Metadata node type
        :param mode: Carries out correlation on 'Taxon' or 'Agglom_Taxon'
        :return:
        """
        var_id = list(type_val.keys())[0]
        # first check if property already exists
        hit = tx.run(("MATCH (a:Property {type: 'spearman_" + var_id +
                      "', name: '" + str(type_val[var_id]) + "'}) RETURN a")).data()
        if len(hit) == 0:
            tx.run(("CREATE (a:Property {type: 'spearman_" + var_id +
                    "', name: '" + str(type_val[var_id]) + "'}) RETURN a"))
        tx.run(("MATCH (a:" + mode +
                "),(b:Property) "
                "WHERE a.name = '" + taxon +
                "' AND b.type = 'spearman_" + var_id +
                "' AND b.name = '" + str(type_val[var_id]) +
                "' CREATE (a)-[r:SPEARMAN]->(b) "
                "RETURN type(r)"))
