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
            if network:
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
        if network:
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
    :param location: File path of tab-delimited file.
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

    If the file cannot be read,
    this function returns False.

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
            logger.warning('Ignoring file with wrong format.', exc_info=True)
            network = False
        if network:
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
                missing_no, edge_dict_tt, edge_dict_tm, edge_dict_mm = \
                    session.read_transaction(self._create_edge_dict, network_id, network)
            with self._driver.session() as session:
                session.write_transaction(self._create_network, network_id)
                session.write_transaction(self._create_edges, tt=edge_dict_tt,
                                          tm=edge_dict_tm, mm=edge_dict_mm, missing_no=missing_no)
        except Exception:
            logger.error("Could not write networkx object to database. \n", exc_info=True)

    def delete_network(self, network_id):
        with self._driver.session() as session:
            edges = session.read_transaction(self._assocs_to_delete, network_id).data()
        edge_query_dict = list()
        for edge in edges:
            edge_query_dict.append({'label': edge['a.name']})
        with self._driver.session() as session:
            session.write_transaction(self._delete_assoc, edge_query_dict)
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
                if weight:
                    try:
                        g.add_edge(index_1, index_2, source=str(edge_list[0][edge]),
                                   weight=float(weight))
                    except ValueError:
                        g.add_edge(index_1, index_2, source=str(edge_list[0][edge]),
                                   weight=weight)
                        edge_error = True
                else:
                    g.add_edge(index_1, index_2, source=str(edge_list[0][edge]))
            if edge_error:
                logger.warning('Could not convert all edge weights to floats for ' + network + '.')
            # necessary for networkx indexing
            with self._driver.session() as session:
                tax_property_dict = session.read_transaction(self._tax_properties_dict)
            tax_nodes = [{'name': x} for x in g.nodes]
            with self._driver.session() as session:
                tax_dict = session.read_transaction(self._tax_query_dict, tax_nodes)
                tax_properties = session.read_transaction(self._tax_properties, tax_nodes, tax_property_dict)
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
        The node properties should be identical for all node dictionaries.
        :param nodes: Dictionary of existing nodes as values with node names as keys.
        :param name: Name of variable, inserted in Neo4j graph database as type
        :param label: Label of source node (e.g. Taxon, Specimen, Property, Experiment etc)
        :param verbose: If true, adds logging info
        :return:
        """
        # first step:
        # check whether key values in node dictionary exist in network
        with self._driver.session() as session:
            network_query = [{'name': x} for x in nodes.keys()]
            matches = session.read_transaction(self._find_nodes, network_query)
            found_nodes = sum([matches[x] for x in matches])
            if found_nodes == 0:
                logger.warning('No source nodes are present in the network. \n')
                sys.exit()
            elif verbose:
                logger.info(str(found_nodes) + ' out of ' + str(len(matches)) + ' values found in database.')
        found_nodes = {x: v for x, v in nodes.items() if matches[x]}
        node_query_dict = list()
        if type(found_nodes[list(found_nodes.keys())[0]]) == dict:
            for node in found_nodes:
                single_query = {'source': node,
                                'target': str(nodes[node]['target']),
                                'name': name}
                for property in found_nodes[node]:
                    if found_nodes[node][property]:
                        single_query[property] = found_nodes[node][property]
                node_query_dict.append(single_query)
            with self._driver.session() as session:
                    # each dictionary value is another dictionary
                    # this dictionary contains the target and weight
                    session.write_transaction(self._create_property,
                                              node_query_dict, sourcetype=label)
        else:
            for node in found_nodes:
                node_query_dict.append({'source': node,
                                        'target': str(nodes[node]['target']),
                                        'name': name})
            with self._driver.session() as session:
                session.write_transaction(self._create_property,
                                          node_query_dict, sourcetype=label)

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
    def _create_edge_dict(tx, name, network):
        """
        Generates all the edges contained in a network and
        connects them to the related network node.
        This function uses NetworkX networks as source.
        :param tx: Neo4j transaction
        :param name: Network name
        :param network: NetworkX object
        :return:
        """
        # first find nodes that are not taxa
        node_list = [{'name': x} for x in network.nodes]
        query = "WITH $batch as batch " \
                "UNWIND batch as record " \
                "MATCH (a:Taxon {name:record.name}) RETURN a.name"
        hits = tx.run(query, batch=node_list).data()
        hits = [x['a.name'] for x in hits]
        missing_no = [{'missingno': x} for x in list(network.nodes) if x not in hits]
        label_dict = {y: 'Taxon' for y in network.nodes}
        for entry in network.nodes:
            if entry in missing_no:
                label_dict[entry] = 'Property'
        edge_query_dict = list()
        for edge in network.edges:
            taxon1 = edge[0]
            taxon2 = edge[1]
            attr = network.get_edge_data(taxon1, taxon2)
            # networkx files imported from graphml will have an index
            # the 'name' property changes the name to the index
            # Indices can overlap across networks,
            # but names should not - OTU_5 in one network should be same as OTU_5 in another
            # if OTU identifiers do not match, users should agglomerate to taxonomic levels
            if 'name' in network.nodes[taxon1]:
                taxon1 = network.nodes[taxon1]['name']
                taxon2 = network.nodes[taxon2]['name']
            # First create / merge the association
            # uid is updated for every edge,
            # faster than checking for uid and adding it
            uid = str(uuid4())
            edge_dict = {'taxon1': taxon1,
                         'taxon2': taxon2,
                         'uuid': uid,
                         'network': name}
            for val in attr:
                edge_dict[val] = attr[val]
            edge_query_dict.append(edge_dict)
        edge_dict_tt = list()
        edge_dict_tm = list()
        edge_dict_mm = list()
        # necessary to make 3 separate queries to add metadata nodes not
        # previously added as taxa
        for query_dict in edge_query_dict:
            if label_dict[query_dict['taxon1']] == 'Taxon' and label_dict[query_dict['taxon2']] == 'Taxon':
                edge_dict_tt.append(query_dict)
            elif label_dict[query_dict['taxon1']] == 'Property' and label_dict[query_dict['taxon2']] == 'Property':
                edge_dict_mm.append(query_dict)
            else:
                new_query = query_dict.copy()
                if label_dict[query_dict['taxon2']] == 'Property':
                    # always put property node as first node
                    new_query['taxon1'] = query_dict['taxon2']
                    new_query['taxon2'] = query_dict['taxon1']
                edge_dict_tm.append(new_query)
        return missing_no, edge_dict_tt, edge_dict_tm, edge_dict_mm

    @staticmethod
    def _create_edges(tx, tt, tm=list(), mm=list(), missing_no=list()):
        """
        Generates all the edges contained in a network and
        connects them to the related network node.
        This function uses dictionaries made by _create_edge_dict as source.


        :param tx: Neo4j transaction
        :param tt: Dictionary of edges between taxa only
        :param tm: Dictionary of edges between metadata and taxa
        :param tm: Dictionary of edges between only metadata
        :param tm: Dictionary of edges between missing nodes
        :return:
        """
        if len(missing_no) > 0:
            # first create missing nodes
            query = "WITH $batch as batch " \
                    "UNWIND batch as record " \
                    "MERGE (a:Property {name:record.missingno}) " \
                    "RETURN a"
            tx.run(query, batch=missing_no)
        query = "WITH $batch as batch " \
                "UNWIND batch as record " \
                "MATCH (a:Taxon {name: record.taxon1}), " \
                "(b:Taxon {name: record.taxon2}) " \
                "MERGE p=(a)<-[:PARTICIPATES_IN]-(e:Edge {weight: record.weight})-[:PARTICIPATES_IN]->(b) " \
                "SET e.name = record.uuid " \
                "RETURN e"
        tx.run(query, batch=tt)
        query = "WITH $batch as batch " \
                "UNWIND batch as record " \
                "MATCH (a:Property {name: record.taxon1}), " \
                "(b:Taxon {name: record.taxon2}) " \
                "MERGE p=(a)<-[:PARTICIPATES_IN]-(e:Edge {weight: record.weight})-[:PARTICIPATES_IN]->(b) " \
                "SET e.name = record.uuid " \
                "RETURN e"
        tx.run(query, batch=tm)
        query = "WITH $batch as batch " \
                "UNWIND batch as record " \
                "MATCH (a:Property {name: record.taxon1}), " \
                "(b:Property {name: record.taxon2}) " \
                "MERGE p=(a)<-[:PARTICIPATES_IN]-(e:Edge {weight: record.weight})-[:PARTICIPATES_IN]->(b) " \
                "SET e.name = record.uuid " \
                "RETURN e"
        tx.run(query, batch=mm)
        query = "WITH $batch as batch " \
                "UNWIND batch as record " \
                "MATCH (a:Edge {name: record.uuid}), " \
                "(b:Network {name: record.network}) " \
                "MERGE p=(a)<-[r:PART_OF]->(b) " \
                "RETURN r"
        tx.run(query, batch=tt)
        tx.run(query, batch=tm)
        tx.run(query, batch=mm)

    @staticmethod
    def _tax_query_dict(tx, nodes):
        """
        Returns a dictionary of taxonomic values for each node.
        :param tx: Neo4j transaction
        :param node: Node name
        :return: Dictionary of taxonomy separated by taxon
        """
        # Cannot query all taxonomies at once since
        # the pattern needs to match all statements,
        # and not all taxa are connected to all levels
        tax_dict = {'Species': None,
                    'Genus': None,
                    'Family': None,
                    'Order': None,
                    'Class': None,
                    'Phylum': None,
                    'Kingdom': None}
        for val in tax_dict:
            query = "WITH $batch as batch " \
                    "UNWIND batch as record " \
                    "MATCH (a:Taxon {name: record.name})--(b:" + val + ") " \
                    "RETURN a.name, b.name"
            vals = tx.run(query, batch=nodes).data()
            tax_dict[val] = vals
        final_dict = dict.fromkeys(tax_dict)
        for val in final_dict:
            final_dict[val] = {x['a.name']: x['b.name'] for x in tax_dict[val]}
        return final_dict

    @staticmethod
    def _tax_properties_dict(tx):
        """
        Constructs a dictionary with property types as keys.
        :param tx: Neo4j transaction
        :return: Dictionary with only keys
        """
        nodes = tx.run("MATCH (n)--(m:Property) WHERE n:Taxon RETURN m").data()
        nodes = [{'name': x} for x in _get_unique(nodes, 'm')]
        properties = dict()
        query = "WITH $batch as batch " \
                "UNWIND batch as record " \
                "MATCH (a:Property {name:record.name}) " \
                "RETURN a"
        properties_data = tx.run(query, batch=nodes).data()
        for prop in properties_data:
            properties[prop['a']['name']] = dict()
        return properties

    @staticmethod
    def _tax_properties(tx, nodes, tax_property_dict):
        """
        Returns a dictionary of taxon / sample properties, to be included as taxon metadata.
        :param tx: Neo4j transaction
        :param nodes: Dictionary with node names
        :param tax_property_dict: Dictionary with taxon property labels
        :return: Dictionary of dictionary of taxon properties
        """
        for property in tax_property_dict:
            tax_property_dict[property]
            query = "WITH $batch as batch " \
                    "UNWIND batch as record " \
                    "MATCH p=(a:Taxon {name: record.name})" \
                    "-[r]-(b:Property {name: '" + property + "'}) " \
                    "RETURN p"
            query_results = tx.run(query, batch=nodes).data()
            for result in query_results:
                tax = result['p'].nodes[0]['name']
                rel = result['p'].relationships[0]['value']
                tax_property_dict[property][tax] = rel
        return tax_property_dict

    @staticmethod
    def _association_list(tx, network):
        """
        Returns a list of edges, as taxon1, taxon2, and, if present, weight.
        :param tx: Neo4j transaction
        :param network: Name of network or set node
        :return: List of lists with source and target nodes, source networks and edge weights.
        """
        try:
            edges = tx.run(("MATCH (n:Edge)--(b {name: '" + network +
                            "'}) RETURN n")).data()
            networks = dict()
            weights = dict()
            edges = [{'name': x['n']['name']} for x in edges]
            query = "WITH $batch as batch " \
                    "UNWIND batch as record " \
                    "MATCH (m)--(p:Edge {name: record.name})--(n) " \
                    "WHERE (m:Taxon OR m:Property) AND (n:Taxon OR n:Property) " \
                    "RETURN p,m,n"
            partner_results = tx.run(query, batch=edges).data()
            taxon_dict = {x['name']: list() for x in edges}
            for result in partner_results:
                taxon_dict[result['p']['name']] = (result['m'].get('name'), result['n'].get('name'))
            query = "WITH $batch as batch " \
                    "UNWIND batch as record " \
                    "MATCH (p:Edge {name: record.name})--(n:Network)" \
                    "RETURN p,n"
            network_results = tx.run(query, batch=edges).data()
            network_dict = {x['name']: list() for x in edges}
            for result in network_results:
                network_dict[result['p']['name']].append(result['n']['name'])
            weight_dict = dict.fromkeys(network_dict.keys())
            for result in network_results:
                weight_dict[result['p']['name']] = result['p']['weight']
            for edge in network_dict:
                networks[taxon_dict[edge]] = network_dict[edge]
                weights[taxon_dict[edge]] = weight_dict[edge]
        except AttributeError:
            logger.warning("Could not extract edges from database.", exc_info=True)
        return networks, weights

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
        found_nodes = {x['name']: False for x in names}
        query = "WITH $batch as batch " \
                "UNWIND batch as record " \
                "MATCH (p {name: record.name}) " \
                "RETURN p, count(p)"
        finding_nodes = tx.run(query, batch=names).data()
        for name in finding_nodes:
            # only checking node name; should be unique in database!
            found_nodes[name['p']['name']] = True
            if name['count(p)'] > 1:
                logger.warning("Duplicated node name in database! \n")
        return found_nodes

    @staticmethod
    def _create_property(tx, query_dict, sourcetype=''):
        """
        Creates target node if it does not exist yet
        and adds the relationship between target and source.
        If the dictionary contains new properties,
        these are added as extra properties on the new target node.
        :param tx: Neo4j transaction
        :param query_dict: Dictionary of values to include as nodes
        :param sourcetype: Label of node to connect to
        :return:
        """
        query = "WITH $batch as batch " \
                "UNWIND batch as record " \
                "MERGE (a:Property {name:record.name"
        property_names = [x for x in query_dict[0].keys()
                          if x not in ['source',
                                       'sourcetype',
                                       'target',
                                       'name']]
        for val in property_names:
            query += ", " + val + ": record." + val
        query += "}) RETURN a"
        tx.run(query, batch=query_dict)
        if len(sourcetype) > 0:
            sourcetype = ':' + sourcetype
        query = "WITH $batch as batch " \
                "UNWIND batch as record " \
                "MATCH (a" + sourcetype + " {name:record.source}), " \
                "(b:Property {name: record.name}) " \
                "MERGE (a)-[r:QUALITY_OF {value: record.target}]->(b) RETURN r"
        tx.run(query, batch=query_dict)

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
    def _delete_assoc(tx, edge_query_dict):
        """
        Deletes an edge node.
        :param tx: Neo4j transaction
        :param edge_query_dict: List of dictionaries with edge names
        :return:
        """
        query = "WITH $batch as batch " \
                "UNWIND batch as record " \
                "MERGE (a:Edge {name:record.label}) DETACH DELETE a"
        tx.run(query, batch=edge_query_dict)

    @staticmethod
    def _delete_disconnected_taxon(tx):
        """
        After deleting a network, some agglomerated nodes may not be represented in the database.
        These disconnected nodes are deleted.
        :param tx:
        :return:
        """
        names = tx.run("MATCH (a:Taxon) WHERE NOT (a)--(:Edge) RETURN a").data()
        del_dict = list()
        for name in names:
            del_dict.append({'label': name['a']['name']})
        query = "WITH $batch as batch " \
                "UNWIND batch as record " \
                "MERGE (a:Taxon {name:record.label}) DETACH DELETE a"
        tx.run(query, batch=del_dict)




