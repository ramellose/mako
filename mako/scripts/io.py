"""
This module contains functions for importing and exporting network files
to and from the Neo4j database.
This includes reading in network files and uploading them,
but also ports the uploaded networks to an open instance of Cytoscape.
Cytoscape is one of the most popular visualization engines for biological data and is frequently used
for gene regulatory networks, microbial association networks and more.
This file contains functions for sending json-formatted data
from the Neo4j database to Cytoscape through the Python requests library.
"""

__author__ = 'Lisa Rottjers'
__maintainer__ = 'Lisa Rottjers'
__email__ = 'lisa.rottjers@kuleuven.be'
__status__ = 'Development'
__license__ = 'Apache 2.0'


from uuid import uuid4  # generates unique IDs for edges + observations
import networkx as nx
from mako.scripts.utils import ParentDriver, _get_unique, _create_logger, _read_config, _get_path
import numpy as np
import pandas as pd
import logging
import sys
import os
import json
import requests
import re

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# handler to sys.stdout
sh = logging.StreamHandler(sys.stdout)
sh.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
sh.setFormatter(formatter)
logger.addHandler(sh)


def start_io(inputs):
    """
    Takes all arguments and processes these to read / write to the Neo4j database.
    Mostly, this is reading / writing networks and deleting networks.

    :param inputs: Dictionary of arguments.
    :return:
    """
    # handler to file
    # construct logger after filepath is provided
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
        sys.exit()
    # Only process network files if present
    if inputs['networks'] and not inputs['delete'] and not inputs['write'] and not inputs['cyto']:
        try:
            for x in inputs['networks']:
                logger.info('Working on ' + x + '...')
                # first check if it is a file or path
                read_networks(files=x, filepath=inputs['fp'], driver=driver)
                # bug, function takes way too long on test data
        except Exception:
            logger.error("Failed to import network files.", exc_info=True)
    if inputs['delete']:
        names = inputs['networks']
        if not names:
            names = [x['a']['name'] for x in driver.query("MATCH (a:Network) RETURN a")]
        for name in names:
            logger.info("Deleting " + name + "...")
            driver.delete_network(name)
        driver.query("MATCH (a:Set) DETACH DELETE a")
    if inputs['write']:
        try:
            driver.export_network(path=inputs['fp'], networks=inputs['networks'])
        except Exception:
            logger.error("Failed to write network files to disk.", exc_info=True)
    if inputs['cyto']:
        try:
            driver.export_cyto(inputs['networks'])
        except Exception:
            logger.error("Failed to port to Cytoscape.", exc_info=True)
    if inputs['fasta']:
        try:
            for location in inputs['fasta']:
                add_sequences(filepath=inputs['fp'], location=location, driver=driver)
        except Exception:
            logger.error("Failed to add FASTA files.", exc_info=True)
    if inputs['meta']:
        try:
            for location in inputs['meta']:
                add_metadata(filepath=inputs['fp'], location=location, driver=driver)
        except Exception:
            logger.error("Failed to add metadata file.", exc_info=True)
    driver.close()
    logger.info('Completed io operations!  ')


def read_networks(files, filepath, driver):
    """
    Reads network files from a list and calls the driver for each file.
    4 ways of giving the filepaths are possible:
        1. A complete filepath to the directory containing BIOMS
        2. A complete filepath to the BIOM file(s)
        3. Filename of network file(s) stored the current working directory
        4. Filename of network file(s) stored in the filepath directory
    The filename can also be a relative filepath.

    :param files: List of network filenames or file directories
    :param filepath: Filepath where files are stored / written
    :param driver: Biom2Neo driver instance
    :return:
    """
    if os.path.isdir(files):
        for y in os.listdir(files):
            network = _read_network_extension(files + '/' + y)
            name = y.split(".")[0]
            driver.convert_networkx(network=network, network_id=name)
    else:
        checked_path = _get_path(path=files, default=filepath)
        if checked_path:
            network = _read_network_extension(checked_path)
        else:
            sys.exit()
        name = files.split('/')[-1]
        name = name.split('\\')[-1]
        name = name.split(".")[0]
        driver.convert_networkx(network=network, network_id=name)


def add_sequences(filepath, location, driver):
    """
    This function opens a folder of FASTA sequences with identifiers
    matching to OTU identifiers in the Neo4j database.
    The FASTA sequences are converted to a dictionary and uploaded to
    the database with the Neo4j driver function include_nodes.
    :param filepath: File path string
    :param location: Folder containing FASTA sequences matching to OTU identifiers.
    I.e. GreenGenes FASTA files are accepted. Complete file paths are also accepted.
    :param driver: IO driver
    :return: Updates database with 16S sequences.
    """
    # get list of taxa in database
    taxa = driver.return_taxa()
    sequence_dict = dict()
    single_file = False
    if os.path.isdir(location):
        logger.info("Found " + str(len(os.listdir(location))) + " files.")
        for y in os.listdir(location):
            for filename in os.listdir(location):
                sequence_dict.update(_convert_fasta(filename))
    else:
        checked_path = _get_path(path=location, default=filepath)
    if checked_path:
        sequence_dict.update(_convert_fasta(filename))
    else:
        sys.exit()
    # with the sequence list, run include_nodes
    seqs_in_database = taxa.intersection(sequence_dict.keys())
    sequence_dict = {k: {'target': v, 'weight': None} for k, v in sequence_dict.items() if k in seqs_in_database}
    logger.info("Uploading " + str(len(sequence_dict)) + " sequences.")
    driver.include_nodes(sequence_dict, name="16S", label="Taxon")


def add_metadata(filepath, location, driver):
    """
    This function reads an edge list, where the left column is a taxon or other node
    in the database, and the right column a new property to add.
    It uses the column names to define property types.
    If there is a third column, this is added as an edge weight.
    :param filepath: File path string
    :param location: Folder containing FASTA sequences matching to OTU identifiers.
    I.e. GreenGenes FASTA files are accepted. Complete file paths are also accepted.
    :param driver: IO driver    :return:
    """
    if os.path.isdir(location):
        logger.info("Found " + str(len(os.listdir(location))) + " files.")
        for y in os.listdir(location):
            for filename in os.listdir(location):
                source, target, value_dict = _convert_table(filename)
                logger.info("Uploading " + str(len(value_dict)) + " values.")
                driver.include_nodes(nodes=value_dict, name=target, label=source)
    else:
        checked_path = _get_path(path=location, default=filepath)
    if checked_path:
        source, target, value_dict = _convert_table(checked_path)
        logger.info("Uploading " + str(len(value_dict)) + " values.")
        driver.include_nodes(nodes=value_dict, name=target, label=source)


def _convert_fasta(filename):
    """
    Reads a FASTA file and converts this to a dictionary.

    :param filename:
    :return:
    """
    sequence_dict = {}
    with open(filename + '//' + filename, 'r') as file:
        lines = file.readlines()
        logger.info("16S file " + filename + " contains " + str(int(len(lines) / 2)) + " sequences.")
    for i in range(0, len(lines), 2):
        otu = lines[i].rstrip()[1:]  # remove > and \n
        sequence = lines[i + 1].rstrip()
        sequence_dict[otu] = sequence
    return sequence_dict


def _convert_table(data):
    """
    Reads a tab-delimited table and converts this into a dictionary that can
    be used by the IO driver include_nodes function.

    :param data: Location of tab-delimited file
    :return:
    """
    data = pd.read_csv(data, sep='\t')
    source = data.columns[0].strip()
    target = data.columns[1].strip()
    data_dict = data.set_index(data.columns[0]).to_dict()
    value_dict = dict()
    for key in data[list(data.keys())[0]]:
        if len(data.columns) == 3:
            value_dict[key] = [data_dict[data.columns[1]][key], data_dict[data.columns[2]][key]]
        else:
            value_dict[key] = [data_dict[data.columns[1]][key], None]
    value_dict = {k: {'target': v[0], 'weight': v[1]} for k, v in value_dict.items()}
    return source, target, value_dict


def _read_network_extension(filename):
    """
    Given a filename with a specific extension,
    this function calls the correct function to read the file.

    :param filename: Complete filename.
    :return: NetworkX object
    """
    extension = filename.split(sep=".")
    extension = extension[len(extension) - 1]
    network = None
    try:
        if extension == 'graphml':
            network = nx.read_graphml(filename)
        elif extension == 'txt':
            network = nx.read_weighted_edgelist(filename)
        elif extension == 'gml':
            network = nx.read_gml(filename)
        else:
            logger.warning('Format not accepted. '
                           'Please specify the filename including extension (e.g. test.graphml).', exc_info=True)
            sys.exit()
        try:
            if 'name' in network.nodes[list(network.nodes)[0]]:
                if network.nodes[list(network.nodes)[0]]['name'] != list(network.nodes)[0]:
                    network = nx.relabel_nodes(network, nx.get_node_attributes(network, 'name'))
        except IndexError:
            logger.warning('One of the imported networks contains no nodes.', exc_info=True)
    except Exception:
        logger.error('Could not import network file!', exc_info=True)
    return network


class IoDriver(ParentDriver):
    """
    Initializes a driver for accessing the Neo4j database.
    This driver uploads, deletes and accesses network files.
    """
    def convert_networkx(self, network_id, network):
        """
        Uploads NetworkX object to Neo4j database.
        :param network_id: Name for network node.
        :param network: NetworkX object.
        :param exp_id: Name of experiment used to generate network.
        :param log: Log of steps carried out to generate network
        :param mode: if 'weight, weighted edges are uploaded
        :return:
        """
        try:
            with self._driver.session() as session:
                session.write_transaction(self._create_network, network_id)
                session.write_transaction(self._create_edges, network_id, network)
        except Exception:
            logger.error("Could not write networkx object to database. \n", exc_info=True)

    def delete_network(self, network_id):
        with self._driver.session() as session:
            edges = session.read_transaction(self._assocs_to_delete, network_id).data()
        with self._driver.session() as session:
            for edge in edges:
                session.write_transaction(self._delete_assoc, edge['a.name'])
        logger.info('Detached edges...')
        self.query(("MATCH (a:Network) WHERE a.name = '" + network_id + "' DETACH DELETE a"))
        with self._driver.session() as session:
            session.write_transaction(self._delete_disconnected_taxon)
        logger.info('Finished deleting ' + network_id + '.')

    def return_networks(self, networks):
        """
        Returns NetworkX networks from the Neo4j database.
        :param networks: Names of networks to return.
        :return: Dictionary of networks
        """
        results = dict()
        if not networks:
            with self._driver.session() as session:
                networks = session.read_transaction(self._query,
                                                    "MATCH (n:Network) RETURN n")
                networks.extend(session.read_transaction(self._query,
                                                         "MATCH (n:Set) RETURN n"))
            networks = list(_get_unique(networks, key='n'))
        # create 1 network per database
        for network in networks:
            g = nx.Graph()
            with self._driver.session() as session:
                edge_list = session.read_transaction(self._association_list, network)
            edge_error = None
            for edge in edge_list[0]:
                index_1 = edge[0]
                index_2 = edge[1]
                weight = edge_list[1][edge]
                try:
                    g.add_edge(index_1, index_2, source=str(edge_list[0][edge]),
                               weight=float(weight))
                except ValueError:
                    g.add_edge(index_1, index_2, source=str(edge_list[0][edge]),
                               weight=weight)
                    edge_error = True
            if edge_error:
                logger.warning('Could not convert all edge weights to floats for ' + network + '.')
            # necessary for networkx indexing
            tax_dict = {}
            tax_levels = ['Kingdom', 'Phylum', 'Class', 'Order', 'Family', 'Genus', 'Species']
            for item in tax_levels:
                tax_dict[item] = dict()
            tax_properties = {}
            with self._driver.session() as session:
                tax_properties = session.read_transaction(self._tax_properties_dict)
            for node in g.nodes:
                with self._driver.session() as session:
                    tax_dict = session.read_transaction(self._tax_dict, node, tax_dict)
                    tax_properties = session.read_transaction(self._tax_properties, node, tax_properties)
            for node in g.nodes:
                with self._driver.session() as session:
                    tax_properties = session.read_transaction(self._tax_properties, node, tax_properties)
            for item in tax_dict:
                nx.set_node_attributes(g, tax_dict[item], item)
            for item in tax_properties:
                nx.set_node_attributes(g, tax_properties[item], item)
            g = g.to_undirected()
            results[network] = g
        return results

    def return_taxa(self):
        """
        Returns taxa from the Neo4j database.
        :return: List of taxa
        """
        with self._driver.session() as session:
            taxa = session.read_transaction(self._get_list, 'Taxon')

    def export_network(self, path, networks=None):
        """
        Writes networks to graphML file.
        If no path is given, the network is returned as a NetworkX object.
        :param path: Filepath where network is written to.
        :param networks: Names of networks to write to disk.
        :return:
        """
        results = None
        try:
            results = self.return_networks(networks)
            if path:
                for network in results:
                    name = path + '/' + network + '.graphml'
                    nx.write_graphml(results[network], name)
                    logger.info('Written network to ' + name + '.')
        except Exception:
            logger.error("Could not write database graph to GraphML file. \n", exc_info=True)
        return results

    def export_cyto(self, networks=None):
        """
        Writes networks to Cytoscape.
        :param networks: Names of networks to write to disk.
        :return:
        """
        if not networks:
            networks = _get_unique(self.query("MATCH (n:Network) RETURN n"), 'n')
            results = self.return_networks(networks)
        else:
            results = self.return_networks(networks)
        # Basic Setup
        port_number = 1234
        base = 'http://localhost:' + str(port_number) + '/v1/'
        headers = {'Content-Type': 'application/json'}
        try:
            for network in results:
                # Define dictionary from networkx
                network_dict = {'data':
                                {"node_default": {}, "edge_default": {}, 'name': network},
                                'elements': {'nodes': [], 'edges': []}}
                i = 1
                id_dict = {}
                for node in results[network].nodes:
                    id_dict[node] = i
                    data = {'data': {'name': node, 'id': str(i),
                                     'SUID': int(i), 'selected': False,  'shared_name': node}}
                    for item in results[network].nodes[node]:
                        data['data'][item] = results[network].nodes[node][item]
                    network_dict['elements']['nodes'].append(data)
                    i += 1
                i = 1
                for edge in results[network].edges:
                    data = {'data': {'shared_name': edge[0] + '->' + edge[1],
                                     'name': edge[0] + '->' + edge[1],
                                     'source': str(id_dict[edge[0]]), 'target': str(id_dict[edge[1]]),
                                     'id': str(i), 'SUID': int(i), 'selected': False},
                            'selected': False}
                    for item in results[network].edges[edge]:
                        if item == 'source':
                            # source is source node in Cytoscape
                            # but network source in Neo4j
                            data['data']['networks'] = results[network].edges[edge][item]
                        else:
                            data['data'][item] = results[network].edges[edge][item]
                    network_dict['elements']['edges'].append(data)
                    i += 1
                res = requests.post(base + 'networks?collection=Neo4jexport', data=json.dumps(network_dict),
                                    headers=headers)
                new_network_id = res.json()['networkSUID']
                print('Network created for ' + network + ': SUID = ' + str(new_network_id))
        except ConnectionError:
            logger.warning("Could not export networks to Cytoscape. Is Cytoscape running?", exc_info=True)

    def include_nodes(self, nodes, name, label, verbose=True):
        """
        Given a dictionary, this function tries to upload
        the file to the Neo4j database.
        The first column of the edgelist should reflect nodes
        already present in the Neo4j graph database,
        while the second column reflects node names that will be added.
        The column names are used to assign node types to the new metadata.
        The dictionary should contain another dictionary of target nodes and edge weights,
        or only a single value (target node).
        :param nodes: Dictionary of existing nodes as values with node names as keys
        :param name: Name of variable, inserted in Neo4j graph database as type
        :param label: Label of source node (e.g. Taxon, Specimen, Property, Experiment etc)
        :param verbose: If true, adds logging info
        :return:
        """
        # first step:
        # check whether key values in node dictionary exist in network
        with self._driver.session() as session:
            matches = session.read_transaction(self._find_nodes, list(nodes.keys()))
            found_nodes = sum([matches[x] for x in matches])
            if found_nodes == 0:
                logger.warning('No source nodes are present in the network. \n')
                sys.exit()
            elif verbose:
                logger.info(str(found_nodes) + ' out of ' + str(len(matches)) + ' values found in database.')
        found_nodes = {x: v for x, v in nodes.items() if matches[x]}
        for node in found_nodes:
            with self._driver.session() as session:
                if type(nodes[node]) == dict:
                    # each dictionary value is another dictionary
                    # this dictionary contains the target and weight
                    session.write_transaction(self._create_property,
                                              source=node, sourcetype=label,
                                              target=str(nodes[node]['target']), name=name,
                                              property_dictionary=nodes[node])
                else:
                    # each dictionary value is the target
                    session.write_transaction(self._create_property,
                                              source=node, sourcetype=label,
                                              target=str(nodes[node]), name=name)

    def export_fasta(self, fp, name):
        """
        This function exports a FASTA file compatible with other tools,
        e.g. PICRUSt2.
        The advantage of using this FASTA file is that it
        only contains taxa present in the database.
        Hence, tools like PICRUSt2 will run much faster.
        While mako cannot directly run PICRUSt2, the below command
        is an example of how you could generate a PICRUSt2 table to provide to mako.
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
    def _create_network(tx, network, exp_id=None, log=None):
        """
        Generates a network node with provenance for every network
        stored in a Nets object.
        :param tx: Neo4j transaction
        :param network: Network name
        :param exp_id: Experiment name
        :param log: Dictionary of operations carried out to generate network
        :return:
        """
        tx.run("MERGE (a:Network {name: '" + network + "'}) "
               "RETURN a")
        if exp_id:
            tx.run(("MATCH (a:Network), (b:Computational_Technique) "
                    "WHERE a.name = '" + network +
                    "' AND b.name = '" + exp_id +
                    "' MERGE (a)-[r:HAS_SUPPORTING_METHOD]->(b) "
                    "RETURN type(r)"))
        if log:
            for metadata in log:
                if metadata in network:
                    tx.run(("MATCH (a:Network)"
                            "WHERE a.name = '" + network +
                            "' SET a.tool = '" + metadata +
                            "' RETURN a"))
                    for network_property in log[metadata]:
                        tx.run(("MATCH (a:Network)"
                                "WHERE a.name = '" + network +
                                "' SET a." + network_property +
                                " = '" + log[metadata][network_property] +
                                "' RETURN a"))
                else:
                    if type(log[metadata]) is not dict:
                        # ensures metadata for other tools is not included
                        tx.run(("MATCH (a:Network)"
                                " WHERE a.name = '" + network +
                                "' SET a." + metadata +
                                " = '" + log[metadata] +
                                "' RETURN a"))

    @staticmethod
    def _create_edges(tx, name, network):
        """
        Generates all the edges contained in a network and
        connects them to the related network node.
        This function uses NetworkX networks as source.
        :param tx: Neo4j transaction
        :param name: Network name
        :param network: NetworkX object
        :return:
        """
        # creates metadata for eventual CoNet feature edges
        for edge in network.edges:
            taxon1 = edge[0]
            taxon2 = edge[1]
            attr = network.get_edge_data(taxon1, taxon2)
            # networkx files imported from graphml will have an index
            # the 'name' property changes the name to the index
            # should probably standardize graphml imports or something
            if 'name' in network.nodes[taxon1]:
                taxon1 = network.nodes[taxon1]['name']
                taxon2 = network.nodes[taxon2]['name']
            # for CoNet imports, the taxa can actually be features
            # need to check this, because these taxa will NOT be in the dataset
            # in that case, we need to create nodes that represent
            # the features
            network_weight = None
            if 'weight' in attr:
                network_weight = attr['weight']
                hit = tx.run(("MATCH p=(a)<--(e:Edge)-->(b) "
                              "WHERE a.name = '" + taxon1 +
                              "' AND b.name = '" + taxon2 +
                              "' AND e.sign = " + str(np.sign(network_weight)) +
                              " RETURN p")).data()

            else:
                hit = tx.run(("MATCH p=(a)<--(e:Edge)-->(b) "
                              "WHERE a.name = '" + taxon1 +
                              "' AND b.name = '" + taxon2 +
                              "' RETURN p")).data()
            # first check if association is already present
            if len(hit) > 0:
                # we upload weights twice
                # once as the sign of the median
                # once as a list of strings
                weights = [str(network_weight)]
                for association in hit:
                    uid = association['p'].nodes[1].get('name')
                    # first check if there is already a link between the association and network
                    network_hit = tx.run(("MATCH p=(a:Edge)--(b:Network) "
                                          "WHERE a.name = '" +
                                          uid +
                                          "' AND b.name = '" + name +
                                          "' RETURN p")).data()
                    database_weight = association['p'].nodes[1].get('weights')
                    try:
                        database_weight = re.findall("[-+]?[.]?[\d]+(?:,\d\d\d)*[.]?\d*(?:[eE][-+]?\d+)?",
                                                     database_weight)
                    except TypeError:
                        if type(database_weight) == list:
                            pass
                        else:
                            database_weight = []
                    weights.extend(database_weight)
                    if len(network_hit) == 0:
                        tx.run(("MATCH (a:Edge), (b:Network) "
                                "WHERE a.name = '" +
                                uid +
                                "' AND b.name = '" + name +
                                "' MERGE (a)-[r:PART_OF]->(b) "
                                "RETURN type(r)"))
                    tx.run(("MATCH (a:Edge) WHERE a.name = '" +
                            uid +
                            "' SET a.weights = " +
                            str(weights) +
                            " RETURN a"))
                    median_weight = np.median([float(x) for x in weights])
                    tx.run(("MATCH (a:Edge) WHERE a.name = '" +
                            uid +
                            "' SET a.weight = " +
                            str(median_weight) +
                            " SET a.sign = " + str(np.sign(median_weight)) +
                            " RETURN a"))
            else:
                uid = str(uuid4())
                # non alphanumeric chars break networkx
                if 'weight' in attr:
                    tx.run("MERGE (a:Edge {name: $id}) "
                           "SET a.weight = $weight SET a.weights = $weight "
                           "SET a.sign = " + str(np.sign(network_weight)) +
                           " RETURN a", id=uid, weight=str(network_weight))
                else:
                    tx.run("MERGE (a:Edge {name: $id}) "
                           "RETURN a", id=uid)
                match = tx.run(("MATCH (a:Edge), (b:Taxon) "
                                "WHERE a.name = '" +
                                uid +
                                "' AND b.name = '" + taxon1 +
                                "' MERGE (a)-[r:PARTICIPATES_IN]->(b) "
                                "RETURN type(r)")).data()
                if len(match) == 0:
                    logger.error("Taxon in network not in database. Cancelling network upload.")
                    sys.exit()
                match = tx.run(("MATCH (a:Edge), (b:Taxon) "
                                "WHERE a.name = '" +
                                uid +
                                "' AND b.name = '" + taxon2 +
                                "' MERGE (a)-[r:PARTICIPATES_IN]->(b) "
                                "RETURN type(r)")).data()
                if len(match) == 0:
                    logger.error("Taxon in network not in database. Cancelling network upload.")
                    sys.exit()
                tx.run(("MATCH (a:Edge), (b:Network) "
                        "WHERE a.name = '" +
                        uid +
                        "' AND b.name = '" + name +
                        "' MERGE (a)-[r:PART_OF]->(b) "
                        "RETURN type(r)"))

    @staticmethod
    def _tax_dict(tx, node, tax_dict):
        """
        Returns a dictionary of taxonomic values for each node.
        :param tx: Neo4j transaction
        :param node: Node name
        :return: Dictionary of taxonomy separated by taxon
        """
        for level in tax_dict:
            tax = None
            level_name = tx.run("MATCH (b {name: '" + node +
                                "'})--(n:"+ level + ") RETURN n").data()
            if len(level_name) != 0:
                tax = level_name[0]['n'].get('name')
            if tax:
                tax_dict[level][node] = tax
        return tax_dict

    @staticmethod
    def _tax_properties_dict(tx):
        """
        Constructs a dictionary with property types as keys.
        :param tx: Neo4j transaction
        :return: Dictionary with only keys
        """
        nodes = tx.run("MATCH (n)--(m:Property) WHERE n:Taxon OR n:Agglom_Taxon RETURN m").data()
        nodes = _get_unique(nodes, 'm')
        properties = dict()
        for node in nodes:
            property = tx.run("MATCH (m:Property) RETURN m").data()
            property_key = property[0]['m']['type']
            properties[property_key] = dict()
        return properties

    @staticmethod
    def _tax_properties(tx, node, tax_properties):
        """
        Returns a dictionary of taxon / sample properties, to be included as taxon metadata.
        :param tx: Neo4j transaction
        :param node: Taxon node label
        :param tax_properties: Dictionary with taxon property types
        :return: Dictionary of dictionary of taxon properties
        """
        hits = tx.run("MATCH (:Taxon {name: '" + node + "'})--(b:Property) RETURN b").data()
        if hits:
            for hit in hits:
                value = hit['b'].get('name')
                try:
                    value = np.round(float(value), 4)
                except ValueError:
                    pass
                tax_properties[hit['b'].get('type')][node] = value
        return tax_properties

    @staticmethod
    def _association_list(tx, network):
        """
        Returns a list of edges, as taxon1, taxon2, and, if present, weight.
        :param tx: Neo4j transaction
        :param network: Name of network or set node
        :return: List of lists with source and target nodes, source networks and edge weights.
        """
        edges = tx.run(("MATCH (n:Edge)--(b {name: '" + network +
                        "'}) RETURN n")).data()
        networks = dict()
        weights = dict()
        for edge in edges:
            taxa = tx.run(("MATCH (m)--(:Edge {name: '" + edge['n'].get('name') +
                           "'})--(n) "
                           "WHERE (m:Taxon OR m:Agglom_Taxon) AND (n:Taxon OR n:Agglom_Taxon) "
                           "AND m.name <> n.name "
                           "RETURN m, n LIMIT 1")).data()
            if len(taxa) == 0:
                pass  # apparently this can happen. Need to figure out why!!
            else:
                edge_tuple = (taxa[0]['m'].get('name'), taxa[0]['n'].get('name'))
                network = tx.run(("MATCH (:Edge {name: '" + taxa[0]['n'].get('name') +
                                  "'})-->(n:Network) RETURN n"))
                network = _get_unique(network, key='n')
                network_list = list()
                for item in network:
                    network_list.append(item)
                weight = edge['n'].get('weight')
                if not weight:
                    weight = edge['n'].get('sign')
                networks[edge_tuple] = network_list
                weights[edge_tuple] = weight
        edge_list = (networks, weights)
        return edge_list

    @staticmethod
    def _get_list(tx, label):
        """
        Returns a list of nodes with the specified label.
        :param tx: Neo4j transaction
        :param label: Neo4j database label of nodes
        :return: List of nodes with specified label.
        """
        results = tx.run(("MATCH (n:" + label + ") RETURN n")).data()
        results = _get_unique(results, key="n")
        return results

    @staticmethod
    def _find_nodes(tx, names):
        """
        Returns True if all nodes in the 'names' list are found in the database.
        :param tx: Neo4j transaction
        :param names: List of names of nodes
        :return:
        """
        found_nodes = dict.fromkeys(names)
        for name in names:
            netname = tx.run("MATCH (n {name: '" + name +
                             "'}) RETURN n").data()
            netname = _get_unique(netname, key='n')
            # only checking node name; should be unique in database!
            if len(netname) == 0:
                found_nodes[name] = False
            elif len(netname) > 1:
                logger.warning("Duplicated node name in database! \n")
            else:
                found_nodes[name] = True
        return found_nodes

    @staticmethod
    def _create_property(tx, source, target, name, sourcetype='', property_dictionary=None):
        """
        Creates target node if it does not exist yet
        and adds the relationship between target and source.
        :param tx: Neo4j transaction
        :param source: Source node, should exist in database
        :param target: Target node
        :param name: Type variable of target node
        :param property_dictionary: Additional property for relationship and targets, tuple
        :return:
        """
        if property_dictionary:
            query = ("MERGE (a:Property {name: '" + str(target) +
                     "', type: '" + name)
            if 'target_property' in property_dictionary:
                for val in property_dictionary['target_property']:
                    query += "', " + val[0] + ": '" + str(val[1])
            query += "'}) RETURN a"
            prop_id = tx.run(query).data()[0]['a'].id
        else:
            prop_id = tx.run(("MERGE (a:Property {name: '" + str(target) + "'}) "
                              "SET a.type = '" + name + "' "
                              "RETURN a")).data()[0]['a'].id
        if len(sourcetype) > 0:
            sourcetype = ':' + sourcetype
        matching_rel = tx.run(("MATCH (a" + sourcetype + ")-[r:QUALITY_OF]-(b:Property) "
                               "WHERE a.name = '" + source +
                               "' AND b.name = '" + target +
                               "' AND b.type = '" + name +
                               "' RETURN r")).data()
        rel = []
        if property_dictionary:
            if 'rel_property' in property_dictionary:
                rel.extend(property_dictionary['rel_property'])
            elif 'weight' in property_dictionary:
                rel.append(('weight',  str(property_dictionary['weight'])))
        else:
            rel = []
        if len(matching_rel) == 0:
            no_rel = "MATCH (a" + sourcetype + "), (b:Property) "
            match = ("WHERE a.name = '" + source +
                     "' AND b.name = '" + target +
                     "' AND b.type = '" + name + "' ")
            if 'target_property' in property_dictionary:
                for val in property_dictionary['target_property']:
                    query = no_rel + match + " AND b." + val[0] + " = '" + str(val[1]) + "' "
            else:
                query = no_rel + match
            query += ("MERGE (a)-[r:QUALITY_OF]->(b) "
                      "RETURN type(r)")
            tx.run(query)
            query = "MATCH (a" + sourcetype + ")-[r]-(b:Property) " + \
                    match
            for val in rel:
                query += ("SET r." + val[0] + " = '" + str(val[1]) + "' ")
            query += "RETURN type(r)"
            tx.run(query)


    @staticmethod
    def _get_fasta(tx):
        """
        Generates a string of FASTA sequences.
        :param tx: Neo4j transaction
        :return: String of FASTA sequences.
        """
        results = tx.run("MATCH (n:Taxon)--(m:Property {type: '16S'}) RETURN n,m").data()
        fasta_dict = {}
        for result in results:
            fasta_dict[result['n']['name']] = result['m']['name']
        fasta_string = str()
        for key in fasta_dict:
            fasta_string += '>' + key + '\n' + fasta_dict[key] + '\n'
        return fasta_string

    @staticmethod
    def _assocs_to_delete(tx, network_id):
        """
        Generates a list of edge nodes linked to the network node that needs to be deleted.
        :param tx: Neo4j transaction
        :param network_id: ID of network node
        :return:
        """
        names = tx.run(("MATCH (a:Edge)--(b:Network) "
                        "WHERE b.name = '" + network_id +
                        "' RETURN a.name"))
        return names

    @staticmethod
    def _delete_assoc(tx, edge):
        """
        Deletes an edge node.
        :param tx: Neo4j transaction
        :param assoc: Edge ID
        :return:
        """
        result = tx.run(("MATCH p=(a:Network)--(:Edge {name: '" + edge +
                         "'})--(b:Network) "
                         "WHERE a.name <> b.name RETURN p")).data()
        if len(result) == 0:
                tx.run(("MATCH (a:Edge {name: '" + edge +
                        "'}) DETACH DELETE a"))

    @staticmethod
    def _delete_disconnected_taxon(tx):
        """
        After deleting a network, some agglomerated nodes may not be represented in the database.
        These disconnected nodes are deleted.
        :param tx:
        :return:
        """
        names = tx.run("MATCH (a:Taxon) WHERE NOT (a)--(:Edge) RETURN a").data()
        for name in names:
            tx.run(("MATCH (a:Taxon) "
                    "WHERE a.name = '" + name['a']['name'] +
                    "' DETACH DELETE a"))

