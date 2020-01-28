"""
This file contains a function for reading in command-line arguments
so BIOM files or tab-delimited files can be read.
The BIOM format is a standardized biological format
that is commonly used to contain biological data.
Tab-delimited files should be supplied with the BIOM-format specified headers (# prefix).

The software can operate in two manners:
import all BIOM files in a folder,
or import separate BIOM files / tab-delimited files

The file also defines a class for a Neo4j driver.
Given a running database, this driver can upload and delete experiments in the database.
"""

__author__ = 'Lisa Rottjers'
__maintainer__ = 'Lisa Rottjers'
__email__ = 'lisa.rottjers@kuleuven.be'
__status__ = 'Development'
__license__ = 'Apache 2.0'

import os
import sys
import numpy as np
from biom import load_table
from biom.parse import MetadataMap
from neo4j.v1 import GraphDatabase
import logging.handlers
from mako.scripts.utils import _create_logger, _read_config

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# handler to sys.stdout
sh = logging.StreamHandler(sys.stdout)
sh.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
sh.setFormatter(formatter)
logger.addHandler(sh)

# handler to file
# only handler with 'w' mode, rest is 'a'
# once this handler is started, the file writing is cleared
# other handlers append to the file


def start_biom(inputs):
    """
    Takes all input and returns a dictionary of biom files.
    If tab-delimited files are supplied, these are combined
    into a biom file. These should be specified in the correct order.
    This is mostly a utility wrapper, as all biom-related functions
    are from biom-format.org.
    :param inputs: Dictionary of arguments.
    :return:
    """
    # handler to file
    # construct logger after filepath is provided
    _create_logger(inputs['fp'])
    config = _read_config(inputs)
    try:
        driver = Biom2Neo(uri=config['address'],
                          user=config['username'],
                          password=config['password'],
                          filepath=inputs['fp'])
    except KeyError:
        logger.error("Login information not specified in arguments.", exc_info=True)
        exit()
    check_arguments(inputs)
    # Only process count files if present
    if inputs['biom_file'] is not None:
        try:
            for x in inputs['biom_file']:
                # first check if it is a file or path
                logger.info('Working on ' + x + '...')
                read_bioms(files=x, filepath=inputs['fp'], driver=driver)
        except Exception:
            logger.error("Failed to import BIOM files.", exc_info=True)
    if inputs['count_table'] is not None:
        try:
            for i in range(len(inputs['count_table'])):
                read_tabs(inputs=inputs, i=i, driver=driver)
        except Exception:
            logger.warning("Failed to combine input files.", exc_info=True)
    if inputs['delete']:
        delete_biom(inputs)


def delete_biom(inputs):
    """
    Removes all  values in the Neo4j database linked to the supplied experiment name.

    :param inputs: Dictionary of inputs.
    :return:
    """
    _create_logger(inputs['fp'])
    config = _read_config(inputs)
    try:
        driver = Biom2Neo(uri=config['address'],
                          user=config['username'],
                          password=config['password'],
                          filepath=inputs['fp'])
    except KeyError:
        logger.error("Login information not specified in arguments.", exc_info=True)
    for name in inputs['delete']:
        driver.delete_biom(name)


def check_arguments(inputs):
    """
    Runs some initial checks before importing;
    for example, whether each OTU table has a matching taxonomy table,
    and if there are sample metadata files, whether each OTU table has one.

    :param inputs: Arguments with files to import
    :return: True if checks passed, False if failed
    """
    if inputs['biom_file'] is not None:
        logger.info('BIOM file(s) to process: ' + ", \n".join(inputs['biom_file']))
    if inputs['otu_table'] is not None:
        logger.info('Tab-delimited OTU table(s) to process: \n' + ", \n".join(inputs['otu_table']))
    if inputs['tax_table'] is not None:
        if len(inputs['otu_table']) is not len(inputs['tax_table']):
            logger.error("Add a taxonomy table for every OTU table!", exc_info=True)
            exit()
    if inputs['sample_meta'] is not None:
        if len(inputs['otu_table']) is not len(inputs['sample_data']):
            logger.error("Add a sample data table for every OTU table!", exc_info=True)
            exit()
    if inputs['otu_meta'] is not None:
        if len(inputs['otu_table']) is not len(inputs['otu_meta']):
            logger.error("Add a metadata table for every OTU table!", exc_info=True)
            exit()
    if inputs['biom_file'] is None and inputs['network'] is None:
        if inputs['otu_table'] is None and inputs['network'] is None:
            logger.error("Please supply either a biom file"
                         ", a tab-delimited OTU table or a network!", exc_info=True)
            exit()


def read_bioms(files, filepath, driver):
    """
    Reads BIOM files from a list and calls the driver for each file.
    4 ways of giving the filepaths are possible:
        1. A complete filepath to the directory containing BIOMS
        2. A complete filepath to the BIOM file(s)
        3. Filename of BIOM file(s) stored the current working directory
        4. Filename of BIOM file(s) stored in the filepath directory
    The filename can also be a relative filepath.

    :param files: List of BIOM filenames or file directories
    :param filepath: Filepath where files are stored / written
    :param driver: Biom2Neo driver instance
    :return:
    """
    if os.path.isdir(files):
        for y in os.listdir(files):
            biomtab = load_table(files + '/' + y)
            name = y.split(".")[0]
            driver.convert_biom(biomfile=biomtab, exp_id=name)
    else:
        if os.path.isfile(files):
            biomtab = load_table(files)
        elif os.path.isfile(os.getcwd() + '/' + files):
            biomtab = load_table(os.getcwd() + '/' + files)
        elif os.path.isfile(filepath + '/' + files):
            biomtab = load_table(filepath + '/' + files)
        else:
            logger.error('Unable to import ' + files + '!\n', exc_info=True)
            exit()
        name = files.split('/')[-1]
        name = name.split('\\')[-1]
        name = name.split(".")[0]
        driver.convert_biom(biomfile=biomtab, exp_id=name)


def read_tabs(inputs, i, driver):
    """
    Reads tab-delimited files from lists of filenames.
    These are then combined into a BIOM file.
    The driver is then called to write the BIOM file to the database.

    :param inputs:
    :param i:
    :param driver:
    :return:
    """
    input_fp = inputs['count_table'][i]
    filepath = inputs['fp']
    sample_metadata_fp = None
    observation_metadata_fp = None
    file_prefix = ''
    if os.path.isfile(input_fp):
        biomtab = load_table(input_fp)
    elif os.path.isfile(os.getcwd() + '/' + input_fp):
        biomtab = load_table(os.getcwd() + '/' + input_fp)
        file_prefix = os.getcwd() + '/'
    elif os.path.isfile(filepath + '/' + input_fp):
        biomtab = load_table(filepath + '/' + input_fp)
        file_prefix = filepath + '/'
    else:
        logger.error('Unable to import ' + input_fp + '!\n', exc_info=True)
    name = input_fp.split('/')[-1]
    name = name.split('\\')[-1]
    name = name.split(".")[0]
    # sample metadata is not mandatory, catches None
    try:
        sample_metadata_fp = file_prefix + inputs['sample_meta'][i]
    except TypeError or KeyError:
        pass
    if sample_metadata_fp is not None:
        sample_f = open(sample_metadata_fp, 'r')
        sample_data = MetadataMap.from_file(sample_f)
        sample_f.close()
        biomtab.add_metadata(sample_data, axis='sample')
    # taxonomy is recommended, many functions don't work without it
    # still capture None
    try:
        observation_metadata_fp = file_prefix + inputs['tax_table'][i]
    except TypeError or KeyError:
        pass
    if observation_metadata_fp is not None:
        obs_f = open(observation_metadata_fp, 'r')
        obs_data = MetadataMap.from_file(obs_f)
        obs_f.close()
        # for taxonomy collapsing,
        # metadata variable needs to be a complete list
        # not separate entries for each tax level
        for b in list(obs_data):
            tax = list()
            for l in list(obs_data[b]):
                tax.append(obs_data[b][l])
                obs_data[b].pop(l, None)
            obs_data[b]['taxonomy'] = tax
        biomtab.add_metadata(obs_data, axis='observation')
    # observation metadata is not mandatory, catches None
    try:
        observation_metadata_fp = file_prefix + inputs['taxon_meta'][i]
    except TypeError or KeyError:
        pass
    if observation_metadata_fp is not None:
        obs_f = open(observation_metadata_fp, 'r')
        obs_data = MetadataMap.from_file(obs_f)
        obs_f.close()
        biomtab.add_metadata(obs_data, axis='observation')
    driver.convert_biom(biomfile=biomtab, exp_id=name)


class Biom2Neo(object):
    def __init__(self, uri, user, password, filepath):
        """
        Initializes a Neo4j driver for interacting with the Neo4j database.
        This driver contains functions for uploading BIOM files to the database,
        and also for writing BIOM files from the database to disk.

        :param uri: Adress of Neo4j database
        :param user: Username for Neo4j database
        :param password: Password for Neo4j database
        :param filepath: Filepath where logs will be written.
        """
        _create_logger(filepath)
        try:
            self._driver = GraphDatabase.driver(uri, auth=(user, password))
        except Exception:
            logger.error("Unable to start Biom2Neo driver. \n", exc_info=True)
            sys.exit()

    def close(self):
        """
        Closes the connection to the database.
        :return:
        """
        self._driver.close()

    def query(self, query):
        """
        Accepts a query and provides the results.
        :param query: String containing Cypher query
        :return: Results of transaction with Cypher query
        """
        output = None
        try:
            with self._driver.session() as session:
                output = session.read_transaction(self._query, query)
        except Exception:
            logger.error("Unable to execute query: " + query + '\n', exc_info=True)
        return output

    def convert_biom(self, biomfile, exp_id):
        """
        Stores a BIOM object in the database.
        :param biomfile: BIOM file.
        :param exp_id: Label of experiment used to generate BIOM file.
        :return:
        """
        try:
            # first check if sample metadata exists
            tax_meta = biomfile.metadata(axis='observation')
            sample_meta = biomfile.metadata(axis='sample')
            with self._driver.session() as session:
                session.write_transaction(self._create_experiment, exp_id)
                for taxon in biomfile.ids(axis='observation'):
                    session.write_transaction(self._create_taxon, taxon, biomfile)
                    tax_index = biomfile.index(axis='observation', id=taxon)
                    if tax_meta:
                        meta = biomfile.metadata(axis='observation')[tax_index]
                        for key in meta:
                            if key != 'taxonomy' and type(meta[key]) == str:
                                session.write_transaction(self.create_property,
                                                          source=taxon, sourcetype='Taxon',
                                                          target=meta[key], name=key)
                for sample in biomfile.ids(axis='sample'):
                    session.write_transaction(self._create_sample, sample, exp_id)
                    sample_index = biomfile.index(axis='sample', id=sample)
                    if sample_meta:
                        meta = biomfile.metadata(axis='sample')[sample_index]
                        # need to clean up these 'if' conditions to catch None properties
                        # there is also a problem with commas + quotation marks here
                        for key in meta:
                            # meta[key] = re.sub(r'\W+', '', str(meta[key]))
                            session.write_transaction(self.create_property,
                                                      source=sample, sourcetype='Specimen',
                                                      target=meta[key], name=key, weight=None)
            obs_data = biomfile.to_dataframe()
            rows, cols = np.where(obs_data.values != 0)
            observations = list()
            for taxon, sample in list(zip(obs_data.index[rows], obs_data.columns[cols])):
                value = obs_data[sample][taxon]
                observations.append((taxon, sample, value))
            with self._driver.session() as session:
                for observation in observations:
                    session.write_transaction(self._create_observation, observation)
        except Exception:
            logger.error("Could not write BIOM file to database. \n", exc_info=True)

    def delete_biom(self, exp_id):
        """
        Takes the experiment ID to remove all samples linked to the experiment.
        :param exp_id: Name of Experiment node to remove
        :return:
        """
        with self._driver.session() as session:
            samples = session.read_transaction(self._samples_to_delete, exp_id)
        with self._driver.session() as session:
            for sample in samples:
                session.write_transaction(self._delete_sample, sample)
        logger.info('Detached samples...')
        with self._driver.session() as session:
            taxa = session.read_transaction(self._taxa_to_delete)
        with self._driver.session() as session:
            for tax in taxa:
                session.write_transaction(self._delete_taxon, tax)
        logger.info('Removed disconnected taxa...')
        self.query(("MATCH a:Experiment WHERE a.name = '" + exp_id + "' DETACH DELETE a"))
        logger.info('Finished deleting ' + exp_id + '.')

    @staticmethod
    def _query(tx, query):
        """
        Processes custom queries.
        :param tx: Neo4j transaction
        :param query: String of Cypher query
        :return: Outcome of transaction
        """
        results = tx.run(query).data()
        return results

    @staticmethod
    def _create_experiment(tx, exp_id):
        """
        Creates a node that represents the Experiment ID.
        :param tx: Neo4j transaction
        :param exp_id: Label for experiment
        :return:
        """
        tx.run("CREATE (a:Experiment) SET a.name = $id", id=exp_id)

    @staticmethod
    def _create_taxon(tx, taxon, biomfile):
        """
        Creates a node that represents a taxon.
        Also generates taxonomy nodes + connects them, and
        includes metadata.
        :param tx: Neo4j transaction
        :param taxon: ID for taxon
        :param biomfile: BIOM file containing count data.
        :return:
        """
        # first check if OTU already exists
        hit = tx.run(("MATCH (a:Taxon {name: '" + taxon +
                      "'}) RETURN a")).data()
        if len(hit) == 0:
            tx.run("CREATE (a:Taxon) SET a.name = $id", id=taxon)
            tax_index = biomfile.index(axis='observation', id=taxon)
            if biomfile.metadata(axis='observation'):
                # it is possible that there is no metadata
                tax_dict = biomfile.metadata(axis='observation')[tax_index]['taxonomy']
                tax_levels = ['Kingdom', 'Phylum', 'Class',
                              'Order', 'Family', 'Genus', 'Species']
                # rel_list = ['IS_KINGDOM', 'IS_PHYLUM', 'IS_CLASS',
                #             'IS_ORDER', 'IS_FAMILY', 'IS_GENUS', 'IS_SPECIES']
                # tree_list = ['PART_OF_KINGDOM', 'PART_OF_PHYLUM', 'PART_OF_CLASS',
                #             'PART_OF_ORDER', 'PART_OF_FAMILY', 'PART_OF_GENUS']
                if str(taxon) is not 'Bin':
                    # define range for which taxonomy needs to be added
                    j = 0
                    if tax_dict:
                        for i in range(len(tax_dict)):
                            level = tax_dict[i]
                            if sum(c.isalpha() for c in level) > 1:
                                # only consider adding as node if there is more
                                # than 1 character in the taxonomy
                                # first request ID to see if the taxonomy node already exists
                                j += 1
                        for i in range(0, j):
                            if tax_dict[i] != 'NA':  # maybe allow user input to specify missing values
                                hit = tx.run(("MATCH (a:" + tax_levels[i] +
                                              " {name: '" + tax_dict[i] +
                                              "'}) RETURN a")).data()
                                if len(hit) == 0:
                                    tx.run(("CREATE (a:" + tax_levels[i] +
                                            " {name: '" + tax_dict[i] +
                                            "', type: 'Taxonomy'}) RETURN a")).data()
                                    if i > 0:
                                        tx.run(("MATCH (a:" + tax_levels[i] +
                                                "), (b:" + tax_levels[i-1] +
                                                ") WHERE a.name = '" + tax_dict[i] +
                                                "' AND b.name = '" + tax_dict[i-1] +
                                                "' CREATE (a)-[r: PART_OF]->(b) "
                                                "RETURN type(r)"))
                                hit = tx.run(("MATCH (a:Taxon)-[r]-(b:" + tax_levels[i] +
                                              ") WHERE a.name = '" + taxon +
                                              "' AND b.name = '" + tax_dict[i] +
                                              "' RETURN type(r)")).data()
                                if len(hit) == 0:
                                    tx.run(("MATCH (a:Taxon), (b:" + tax_levels[i] +
                                            ") WHERE a.name = '" + taxon +
                                            "' AND b.name = '" + tax_dict[i] +
                                            "' CREATE (a)-[r: BELONGS_TO]->(b) "
                                            "RETURN type(r)"))
            else:
                tx.run("CREATE (a:Taxon) SET a.name = $id", id='Bin')

    @staticmethod
    def _create_sample(tx, sample, exp_id):
        """
        Creates sample nodes and link to experiment.
        :param tx: Neo4j transaction
        :param sample: Specimen name
        :param exp_id: Experiment name
        :return:
        """
        tx.run("CREATE (a:Specimen) SET a.name = $id", id=sample)
        tx.run(("MATCH (a:Specimen), (b:Experiment) "
                "WHERE a.name = '" + sample +
                "' AND b.name = '" + exp_id +
                "' CREATE (a)-[r:IN_EXPERIMENT]->(b) "
                "RETURN type(r)"))

    @staticmethod
    def create_property(tx, source, target, name, weight, sourcetype=''):
        """
        Creates target node if it does not exist yet
        and adds the relationship between target and source.
        :param tx: Neo4j transaction
        :param source: Source node, should exist in database
        :param target: Target node
        :param name: Type variable of target node
        :param weight: Weight of relationship
        :param sourcetype: Type variable of source node (not required)
        :return:
        """
        hit = tx.run(("MATCH (a:Property) WHERE a.name = '" +
                      target + "' AND a.type = '" +
                      name + "' RETURN a")).data()
        if len(hit) == 0:
            tx.run(("CREATE (a:Property) "
                    "SET a.name = '" + target +
                    "' SET a.type = '" + name + "' "
                                                "RETURN a")).data()
        if len(sourcetype) > 0:
            sourcetype = ':' + sourcetype
        matching_rel = tx.run(("MATCH (a" + sourcetype + ")-[r:HAS_PROPERTY]-(b:Property) "
                               "WHERE a.name = '" + source +
                               "' AND b.name = '" + target +
                               "' AND b.type = '" + name +
                               "' RETURN r")).data()
        if weight:
            rel = " {weight: [" + weight + "]}"
        else:
            rel = ""
        if len(matching_rel) == 0:
            tx.run(("MATCH (a" + sourcetype + "), (b:Property) "
                    "WHERE a.name = '" + source +
                    "' AND b.name = '" + target +
                    "' AND b.type = '" + name +
                    "' CREATE (a)-[r:HAS_PROPERTY" + rel + "]->(b) "
                    "RETURN type(r)"))

    @staticmethod
    def _create_observation(tx, observation):
        """
        Creates relationships between taxa and samples
        that represent the count number of that taxon in a sample.
        :param tx: Neo4j transaction
        :param observation: An observation (count) of a taxon in a sample.
        :return:
        """
        taxon, sample, value = observation
        tx.run(("MATCH (a:Taxon), (b:Specimen) "
                "WHERE a.name = '" + taxon +
                "' AND b.name = '" + sample +
                "' CREATE (a)-[r:FOUND_IN]->(b) "
                "SET r.count = '" + str(value) +
                "' RETURN type(r)"))

    @staticmethod
    def _samples_to_delete(tx, exp_id):
        """
        Generates a list of sample nodes linked to the experiment node that needs to be deleted.
        :param tx:
        :param exp_id: ID of experiment node
        :return:
        """
        names = tx.run(("MATCH (a:Sample)--(b:Experiment) "
                        "WHERE b.name = '" + exp_id +
                        "' RETURN a.name"))
        return names

    @staticmethod
    def _taxa_to_delete(tx):
        """
        After deleting samples, some taxa will no longer
        be present in any experiment. These disconnected taxa need to be removed
        and this function generates the list that does this.
        :param tx:
        :return:
        """
        names = tx.run("MATCH (a:Taxon) WHERE NOT (a)--(:Sample) RETURN a.name")
        return names

    @staticmethod
    def _delete_sample(tx, sample):
        """
        Deletes a sample node and all the observations linked to the sample.
        :param tx:
        :param sample: Sample ID
        :return:
        """
        tx.run(("MATCH (a:Sample) "
                "WHERE a.name = '" + sample +
                "' DETACH DELETE a"))

    @staticmethod
    def _delete_taxon(tx, taxon):
        """
        Deletes a taxon and all the associations linked to the taxon.
        :param tx:
        :param taxon: Taxon ID
        :return:
        """
        tx.run(("MATCH (a:Taxon)--(b:Association) "
                "WHERE a.name = '" + taxon +
                "' DETACH DELETE b"))
        tx.run(("MATCH (a:Taxon) "
                "WHERE a.name = '" + taxon +
                "' DETACH DELETE a"))