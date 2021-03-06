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
import pandas as pd
from biom import load_table
import zipfile
import yaml
import tempfile
import shutil
from pathlib import Path
from biom.parse import MetadataMap
import logging.handlers
from mako.scripts.utils import ParentDriver, _create_logger, \
    _read_config, _get_path, _run_subbatch

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
    if inputs['store_config']:
        config = _read_config(inputs)
    else:
        config = inputs
    try:
        driver = Biom2Neo(uri=config['address'],
                          user=config['username'],
                          password=config['password'],
                          filepath=inputs['fp'],
                          encrypted=inputs['encryption'])
    except KeyError:
        logger.error("Login information not specified in arguments.", exc_info=True)
        sys.exit()
    check_arguments(inputs)
    # Only process count files if present
    if inputs['biom_file'] is not None:
        try:
            for x in inputs['biom_file']:
                # first check if it is a file or path
                logger.info('Working on ' + x + '...')
                read_bioms(files=x, filepath=inputs['fp'], driver=driver, obs=inputs['obs'])
        except Exception:
            logger.error("Failed to import BIOM files.", exc_info=True)
    if inputs['qza'] is not None:
        try:
            for x in inputs['qza']:
                # first check if it is a file or path
                logger.info('Working on ' + x + '...')
                read_qiime2(files=x, filepath=inputs['fp'], driver=driver)
        except Exception:
            logger.error("Failed to import Qiime 2 artifact.", exc_info=True)
    if inputs['count_table'] is not None:
        try:
            for i in range(len(inputs['count_table'])):
                name, biomtab = read_tabs(inputs=inputs, i=i)
                driver.convert_biom(biomfile=biomtab, exp_id=name, obs=inputs['obs'])
        except Exception:
            logger.warning("Failed to combine input files.", exc_info=True)
    elif inputs['tax_table'] is not None:
        try:
            for x in inputs['tax_table']:
                logger.info('Working on uploading separate taxonomy table ' + x + '...')
                name, taxtab = read_taxonomy(filename=x, filepath=inputs['fp'])
                driver.convert_taxonomy(taxonomy_table=taxtab, exp_id=name)
        except Exception:
            logger.warning("Failed to upload taxonomy table.", exc_info=True)
    if inputs['delete']:
        for name in inputs['delete']:
            driver.delete_biom(name)
    driver.close()
    logger.info('Completed neo4biom operations!  ')


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
    if inputs['qza'] is not None:
        logger.info('Qiime 2 archive file(s) to process: ' + ", \n".join(inputs['qza']))
    if inputs['count_table'] is not None:
        logger.info('Tab-delimited OTU table(s) to process: \n' + ", \n".join(inputs['count_table']))
    if inputs['tax_table'] is not None:
        logger.info('Tab-delimited taxonomy table(s) to process: \n' + ", \n".join(inputs['tax_table']))
    if inputs['sample_meta'] is not None:
        if len(inputs['count_table']) is not len(inputs['sample_data']):
            logger.error("Add a sample data table for every OTU table!", exc_info=True)
            sys.exit()
    if inputs['taxon_meta'] is not None:
        if len(inputs['count_table']) is not len(inputs['taxon_meta']):
            logger.error("Add a metadata table for every OTU table!", exc_info=True)
            sys.exit()


def read_bioms(files, filepath, driver, obs=True):
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
    :param obs: If false, counts aren't uploaded.
    :param driver: Biom2Neo driver instance
    :return:
    """
    if os.path.isdir(files):
        for y in os.listdir(files):
            biomtab = load_table(files + '/' + y)
            name = y.split(".")[0]
            driver.convert_biom(biomfile=biomtab, exp_id=name)
    else:
        checked_path = _get_path(path=files, default=filepath)
        if checked_path:
            biomtab = load_table(checked_path)
            name = files.split('/')[-1]
            name = name.split('\\')[-1]
            name = name.split(".")[0]
            driver.convert_biom(biomfile=biomtab, exp_id=name)
        else:
            logger.error("Unable to read BIOM file, path is incorrect.")
            sys.exit()


def read_tabs(inputs, i):
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
    checked_path = _get_path(path=input_fp, default=filepath)
    if checked_path:
        biomtab = load_table(checked_path)
        file_prefix = ''
        if os.path.isfile(os.getcwd() + '/' + input_fp):
            file_prefix = os.getcwd() + '/'
        elif os.path.isfile(filepath + '/' + input_fp):
            file_prefix = filepath + '/'
    else:
        logger.warning("Failed to combine input files.", exc_info=True)
        sys.exit()
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
    return name, biomtab


def read_taxonomy(filename, filepath):
    """
    Reads tab-delimited file representing a taxonomy table.
    :param filename: Full or incomplete filepath to taxonomy
    :param filepath: Extra filepath
    :return:
    """
    taxtab = None
    checked_path = _get_path(path=filename, default=filepath)
    if checked_path:
        taxtab = pd.read_csv(checked_path, sep='\t', index_col=0)
    else:
        logger.warning("Failed to read taxonomy table.", exc_info=True)
        sys.exit()
    name = filename.split('/')[-1]
    name = name.split('\\')[-1]
    name = name.split(".")[0]
    # sample metadata is not mandatory, catches None
    return name, taxtab


def read_qiime2(files, filepath, driver):
    """
    Reads a qza Qiime2 artifact and writes this to the Neo4j database.
    The type information is used to create a new node label in the Neo4j database.
    The uuid is used to create an Experiment node that links the artifact data.

    If the artifact is an OTU table,
    the import proceeds as if it was a BIOM file.
    If the artifact is a taxonomy table,
    the import proceeds as if it was a tab-delimited taxonomy file.

    To avoid installing all of Qiime 2 as a dependency,
    mako contains some utility functions that handle the unzipping
    and reading of the Artifact files.

    :param files: List of BIOM filenames or file directories
    :param filepath: Filepath where files are stored / written
    :param driver: Biom2Neo driver instance
    :return:
    """
    if os.path.isdir(files):
        for y in os.listdir(files):
            filepath = files + '/' + y
            _upload_qiime2(filepath, driver)
    else:
        checked_path = _get_path(path=files, default=filepath)
        if checked_path:
            _upload_qiime2(checked_path, driver)
        else:
            logger.error("Unable to read qza file, path is incorrect.")
            sys.exit()


def _upload_qiime2(filepath, driver):
    artifact, file = _load_qiime2(filepath)
    if artifact['type'] == 'FeatureTable[Frequency]':
        name = artifact['uuid']
        driver.convert_biom(biomfile=file, exp_id=name)
        driver.query("MATCH (n:Experiment {name: '" + name +
                     "'}) SET n.type = '" + artifact['type'] +
                     "' SET n.format = '" + artifact['format'] +
                     "' RETURN n.format")
    elif artifact['type'] == 'FeatureData[Taxonomy]':
        name = artifact['uuid']
        driver.convert_taxonomy(file, name)


def _load_qiime2(filepath):
    """
    Loads a Qiime2 Artifact object and
    returns this object as a tuple of metadata and BIOM table.

    :param filepath: Complete filepath to Qiime2 object
    :return:
    """
    filepath = Path(filepath)
    # create temp directory
    dirpath = tempfile.mkdtemp()
    if zipfile.is_zipfile(str(filepath)):
        with zipfile.ZipFile(str(filepath), mode='r') as file:
            file.extractall(dirpath)
    else:
        shutil.rmtree(dirpath)
        logger.error("This file is not an archive, quitting mako.")
        sys.exit()
    toplevel = os.listdir(dirpath)[0]
    # read metadata
    with open(dirpath + "/" + toplevel + "/metadata.yaml", 'r') as stream:
        artifact = yaml.safe_load(stream)
    try:
        if artifact['type'] == 'FeatureTable[Frequency]':
            file = load_table(dirpath + "/" + toplevel + "/data/feature-table.biom")

        elif artifact['type'] == 'FeatureData[Taxonomy]':
            file = pd.read_table(dirpath + "/" + toplevel + "/data/taxonomy.tsv", sep='\t')
            file[['Kingdom', 'Phylum', 'Class', 'Order', 'Family', 'Genus', 'Species']] = \
                file['Taxon'].str.split('; ', 7, expand=True)
            file = file.set_index('Feature ID')
            file = file.drop(['Taxon', 'Confidence'], axis=1)
        else:
            logger.error("Archive type " + artifact['type']+ " not supported by mako.")
            sys.exit()
    except FileNotFoundError:
        shutil.rmtree(dirpath)
        logger.error("Could not find a feature-table file in the archive.")
        sys.exit()
    # delete temp directory
    shutil.rmtree(dirpath)
    return artifact, file


class Biom2Neo(ParentDriver):
    """
    Initializes a Neo4j driver for interacting with the Neo4j database.
    This driver contains functions for uploading BIOM files to the database,
    and also for writing BIOM files from the database to disk.
    """
    def convert_biom(self, biomfile, exp_id, obs=True):
        """
        Stores a BIOM object in the database.
        To speed up this process, all data from the BIOM object is first converted to
        dictionaries or lists that can be used in parameterized batch queries.
        Labels need to be set statically,
        which is done as part of the static functions.
        If obs is set to false, all taxa are only connected to a single "mock" sample.
        This can lead to a rapid speed-up for data set uploading,
        if sample counts are not necessary.

        :param biomfile: BIOM file.
        :param exp_id: Label of experiment used to generate BIOM file.
        :param obs: Relationships between samples and taxa are only created if obs is set to True.
        :return:
        """
        try:
            # first check if sample metadata exists
            with self._driver.session() as session:
                session.write_transaction(self._create_experiment, exp_id)
            taxon_query_dict = self._create_taxon_dict(biomfile)
            with self._driver.session() as session:
                # Add taxon nodes
                session.write_transaction(self._create_taxon, taxon_query_dict)
            tax_levels = ['Kingdom', 'Phylum', 'Class', 'Order', 'Family', 'Genus', 'Species']
            try:
                taxonomy_table = biomfile.metadata_to_dataframe(axis='observation').drop_duplicates()
                # default naming scheme filters columns
                matching_indices = set(taxonomy_table.columns).intersection(['taxonomy_0', 'taxonomy_1', 'taxonomy_2',
                                                 'taxonomy_3', 'taxonomy_4', 'taxonomy_5',
                                                 'taxonomy_6'])
                taxonomy_table = taxonomy_table[matching_indices]
                taxonomy_table = taxonomy_table.reindex(sorted(taxonomy_table.columns), axis=1)
                for i in reversed(range(len(matching_indices))):
                    level = tax_levels[i]
                    taxonomy_query_dict = self._create_taxonomy_dict(taxonomy_table, i)
                    with self._driver.session() as session:
                        session.write_transaction(self._create_taxonomy, level, taxonomy_query_dict)
                for i in reversed(range(1, len(matching_indices))):
                    # Connect each taxonomic label to its higher-level label
                    lower_level = tax_levels[i]
                    upper_level = tax_levels[i-1]
                    taxonomy_query_dict = self._connect_taxonomy_dict(taxonomy_table, i)
                    with self._driver.session() as session:
                        session.write_transaction(self._connect_taxonomy, lower_level, upper_level,
                                                  taxonomy_query_dict)
                for i in range(len(matching_indices)):
                    taxonomy_query_dict = self._add_taxonomy_dict(biomfile, i)
                    if len(taxonomy_query_dict) > 0:
                        with self._driver.session() as session:
                            session.write_transaction(self._add_taxonomy, tax_levels[i], taxonomy_query_dict)
            except KeyError:
                pass
            try:
                tax_meta = biomfile.metadata_to_dataframe(axis='observation')
                metadata_query_dict1, metadata_query_dict2 = self._create_meta_dict(tax_meta, biomfile)
                if len(metadata_query_dict1) > 0:
                    with self._driver.session() as session:
                        session.write_transaction(self._create_property, metadata_query_dict1)
                if len(metadata_query_dict2) > 0:
                    with self._driver.session() as session:
                        session.write_transaction(self._connect_property, metadata_query_dict2, sourcetype='Taxon')
            except KeyError:
                pass
            sampledata_query_dict1, sampleproperty_query_dict2, sampleproperty_query_dict3 = self._create_sample_dict(biomfile, exp_id)
            if len(sampledata_query_dict1) > 0:
                with self._driver.session() as session:
                    session.write_transaction(self._create_sample, sampledata_query_dict1)
            if len(sampleproperty_query_dict2) > 0:
                with self._driver.session() as session:
                    session.write_transaction(self._create_property, sampleproperty_query_dict2)
            with self._driver.session() as session:
                session.write_transaction(self._create_indices)
            if len(sampleproperty_query_dict3) > 0:
                with self._driver.session() as session:
                    session.write_transaction(self._connect_property, sampleproperty_query_dict3, sourcetype='Specimen')
            if obs:
                observations = self._create_obs_dict(biomfile)
            else:
                observations = self._create_obs_dict_alt(tax_meta, exp_id)
            with self._driver.session() as session:
                session.write_transaction(self._create_observations, observations)
        except Exception:
            logger.error("Could not write BIOM file to database. \n", exc_info=True)

    def convert_taxonomy(self, taxonomy_table, exp_id):
        """
        Stores a taxonomy dataframe in the database.
        To speed up this process, all data from the taxonomy table is first converted to
        dictionaries or lists that can be used in parameterized batch queries.
        Labels need to be set statically,
        which is done as part of the static functions.
        :param taxonomy_table: Pandas dataframe of taxonomy
        :param exp_id: Label of experiment used to generate taxonomy file.
        :return:
        """
        try:
            # first check if sample metadata exists
            with self._driver.session() as session:
                session.write_transaction(self._create_experiment, exp_id)
            taxon_query_dict = self._create_taxon_dict_alt(taxonomy_table)
            with self._driver.session() as session:
                # Add taxon nodes
                session.write_transaction(self._create_taxon, taxon_query_dict)
            tax_levels = ['Kingdom', 'Phylum', 'Class', 'Order', 'Family', 'Genus', 'Species']
            try:
                for i in reversed(range(len(taxonomy_table.columns))):
                    level = tax_levels[i]
                    taxonomy_query_dict = self._create_taxonomy_dict(taxonomy_table, i)
                    with self._driver.session() as session:
                        session.write_transaction(self._create_taxonomy, level, taxonomy_query_dict)
                for i in reversed(range(1, len(taxonomy_table.columns))):
                    # Connect each taxonomic label to its higher-level label
                    lower_level = tax_levels[i]
                    upper_level = tax_levels[i-1]
                    taxonomy_query_dict = self._connect_taxonomy_dict(taxonomy_table, i)
                    with self._driver.session() as session:
                        session.write_transaction(self._connect_taxonomy, lower_level, upper_level,
                                                  taxonomy_query_dict)
                for i in range(len(taxonomy_table.columns)):
                    taxonomy_query_dict = self._add_taxonomy_dict_alt(taxonomy_table, i)
                    if len(taxonomy_query_dict) > 0:
                        with self._driver.session() as session:
                            session.write_transaction(self._add_taxonomy, tax_levels[i], taxonomy_query_dict)
                with self._driver.session() as session:
                    session.write_transaction(self._create_ref_sample, exp_id)
                observations = self._create_obs_dict_alt(taxonomy_table, exp_id)
                with self._driver.session() as session:
                    session.write_transaction(self._create_observations, observations)
            except KeyError:
                pass
        except Exception:
            logger.error("Could not write taxonomy file to database. \n", exc_info=True)

    def delete_biom(self, exp_id):
        """
        Takes the experiment ID to remove all samples linked to the experiment.
        :param exp_id: Name of Experiment node to remove
        :return:
        """
        with self._driver.session() as session:
            samples = session.read_transaction(self._samples_to_delete, exp_id)
        deletion_dict = list()
        for sample in samples:
            deletion_dict.append({'sample': sample['a.name'], 'exp_id': exp_id})
        with self._driver.session() as session:
            session.write_transaction(self._delete_sample, deletion_dict)
        logger.info('Detached samples...')
        with self._driver.session() as session:
            taxa = session.read_transaction(self._taxa_to_delete)
        deletion_dict = list()
        for tax in taxa:
            deletion_dict.append({'taxon': tax['a.name']})
        with self._driver.session() as session:
                session.write_transaction(self._delete_taxon, deletion_dict)
        logger.info('Removed disconnected taxa...')
        self.write(("MATCH (a:Experiment {name: '" + exp_id + "'}) DETACH DELETE a"))
        logger.info('Finished deleting ' + exp_id + '.')

    @staticmethod
    def _create_taxon_dict(biomfile):
        """
        Creates a taxon dictionary to use for batch Neo4j queries.
        This dictionary creates taxon nodes.
        :param biomfile: BIOM object
        :return:
        """
        taxon_query_dict = list()
        for taxon in biomfile.ids(axis='observation'):
            taxon_query_dict.append({'taxon': taxon})
        return taxon_query_dict

    @staticmethod
    def _create_taxon_dict_alt(taxtab):
        """
        Creates a taxon dictionary to use for batch Neo4j queries.
        This dictionary creates taxon nodes.
        :param taxtab: Pandas dataframe of taxonomy
        :return:
        """
        taxon_query_dict = list()
        for taxon in taxtab.index:
            taxon_query_dict.append({'taxon': taxon})
        return taxon_query_dict

    @staticmethod
    def _create_taxonomy_dict(taxonomy_table, i):
        """
        Creates a taxonomy dictionary to use for batch Neo4j queries.
        This dictionary creates taxonomy nodes.
        :param taxonomy_table: Taxonomy dataframe
        :param i: index of taxonomic level
        :return:
        """
        taxonomy_table_i = set(taxonomy_table.iloc[:, i])
        # Create each taxonomic label
        taxonomy_query_dict = list()
        for val in taxonomy_table_i:
            # filters out empty assignments with just prefix or None
            if val and not pd.isna(val):
                if len(val) > 4:
                    taxonomy_query_dict.append({'label': val})
        return taxonomy_query_dict

    @staticmethod
    def _connect_taxonomy_dict(taxonomy_table, i):
        """
        Creates a taxonomy dictionary to use for batch Neo4j queries.
        This dictionary connects taxonomy nodes.
        :param taxonomy_table: Taxonomy dataframe
        :param i: index of taxonomic level
        :return:
        """
        taxonomy_query_dict = list()
        taxonomy_table_i = taxonomy_table.iloc[:, [i, i - 1]].drop_duplicates()
        for index, row in taxonomy_table_i.iterrows():
            # filters out empty assignments with just prefix
            if row[0] and not pd.isna(row[0]):
                if len(row[0]) > 4:
                    taxonomy_query_dict.append({'label1': row[0], 'label2': row[1]})
        return taxonomy_query_dict

    @staticmethod
    def _add_taxonomy_dict(biomfile, i):
        """
        Creates a taxonomy dictionary to connect taxa to taxonomy nodes.
        :param biomfile: BIOM object
        :param i: Index of taxonomy level
        :return:
        """
        taxonomy_query_dict = list()
        for taxon in biomfile.ids(axis='observation'):
            tax_index = biomfile.index(axis='observation', id=taxon)
            tax_labels = biomfile.metadata(axis='observation')[tax_index]['taxonomy']
            if tax_labels[i] and not pd.isna(tax_labels[i]):
                if len(tax_labels[i]) > 4:
                    taxonomy_query_dict.append({'taxon': taxon, 'level': tax_labels[i]})
        return taxonomy_query_dict

    @staticmethod
    def _add_taxonomy_dict_alt(taxonomy_table, i):
        """
        Creates a taxonomy dictionary to connect taxa to taxonomy nodes.
        :param taxonomy_table: Pandas dataframe
        :param i: Index of taxonomy level
        :return:
        """
        taxonomy_query_dict = list()
        for j in range(len(taxonomy_table.index)):
            tax_name = list(taxonomy_table.index)[j]
            tax_label = taxonomy_table.iloc[j][i]
            if tax_label and not pd.isna(tax_label):
                if len(tax_label) > 4:
                    taxonomy_query_dict.append({'taxon': tax_name, 'level': tax_label})
        return taxonomy_query_dict

    @staticmethod
    def _create_meta_dict(tax_meta, biomfile):
        """
        Takes taxon metadata (not taxonomy) and makes a dictionary for those metadata properties.
        :param tax_meta: Dataframe of taxon metadata.
        :param biomfile: BIOM object.
        :return:
        """
        metadata_query_dict1 = list()
        for column in tax_meta.columns:
            if 'taxonomy' not in column:
                metadata_query_dict1.append({'label': column})
        metadata_query_dict2 = list()
        for taxon in biomfile.ids(axis='observation'):
            tax_index = biomfile.index(axis='observation', id=taxon)
            if len(tax_meta) > 0:
                meta = biomfile.metadata(axis='observation')[tax_index]
                for key in meta:
                    if key != 'taxonomy' and type(meta[key]) == str:
                        metadata_query_dict2.append({'source': taxon,
                                                    'value': meta[key], 'name': key})
        return metadata_query_dict1, metadata_query_dict2

    @staticmethod
    def _create_sample_dict(biomfile, exp_id):
        """
        Creates two dictionaries, one for sample nodes, one for sample properties.
        :param biomfile: BIOM object
        :param exp_id: Name used for BIOM object
        :return:
        """
        sampledata_query_dict = list()
        sampleproperty_query_dict = list()
        sampleproperty_query_dict2 = list()
        for sample in biomfile.ids(axis='sample'):
            sampledata_query_dict.append({'sample': sample, 'exp_id': exp_id})
        try:
            sample_meta = biomfile.metadata_to_dataframe(axis='sample')
            for column in sample_meta.columns:
                sampleproperty_query_dict.append({'label': column})
            for sample in biomfile.ids(axis='sample'):
                sample_index = biomfile.index(axis='sample', id=sample)
                if len(sample_meta) > 0:
                    meta = biomfile.metadata(axis='sample')[sample_index]
                    # need to clean up these 'if' conditions to catch None properties
                    # there is also a problem with commas + quotation marks here
                    for key in meta:
                        sampleproperty_query_dict2.append({'source': sample,
                                                           'value': meta[key], 'name': key})
        except KeyError:
            pass
        return sampledata_query_dict, sampleproperty_query_dict, sampleproperty_query_dict2

    @staticmethod
    def _create_obs_dict(biomfile):
        """
        Creates a dictionary that can be used to connect taxa to samples via observation values.
        :param biomfile: BIOM object.
        :return:
        """
        obs_data = biomfile.matrix_data
        data = [pd.Series(obs_data[i].toarray().ravel()) for i in np.arange(obs_data.shape[0])]
        obs_data = pd.DataFrame(data, index=biomfile.ids(axis='observation'))
        obs_data.columns = biomfile.ids(axis='sample')
        rows, cols = np.where(obs_data.values != 0)
        observations = list()
        for taxon, sample in list(zip(obs_data.index[rows], obs_data.columns[cols])):
            value = obs_data[sample][taxon]
            observations.append({'taxon': taxon, 'sample': sample, 'value': value})
        return observations

    @staticmethod
    def _create_obs_dict_alt(taxonomy_table, exp_id):
        """
        Creates a dictionary that can be used to connect taxa to samples via observation values.
        :param taxonomy_table: Taxonomy table.
        :param exp_id: Name of experiment node
        :return:
        """
        observations = list()
        for taxon in taxonomy_table.index:
            observations.append({'taxon': taxon, 'sample': exp_id, 'value': 0})
        return observations

    @staticmethod
    def _create_experiment(tx, exp_id):
        """
        Creates a node that represents the Experiment ID.
        :param tx: Neo4j transaction
        :param exp_id: Label for experiment
        :return:
        """
        tx.run("MERGE (a:Experiment {name: '" + exp_id + "'}) RETURN a")

    @staticmethod
    def _create_ref_sample(tx, exp_id):
        """
        Creates a fake sample used to match the data schema
        for taxonomy tables uploaded without observational data.
        :param exp_id: Name of Experiment node
        :return:
        """
        tx.run("MERGE (a:Specimen {name: '" + exp_id + "'}) RETURN a")
        tx.run(("MATCH (a:Specimen {name:'" + exp_id +
                "'}), (b:Experiment {name:'" + exp_id +
                "'}) MERGE (a)-[r:PART_OF]->(b) RETURN type(r)"))

    @staticmethod
    def _create_taxon(tx, taxon_query_dict):
        """
        Creates a node that represents a taxon.
        :param tx: Neo4j transaction
        :param taxon_query_dict: Dictionary of taxon IDs
        :return:
        """
        query = "WITH $batch as batch \
        UNWIND batch as record \
        MERGE (a:Taxon {name:record.taxon}) RETURN a"
        _run_subbatch(tx, query, taxon_query_dict)

    @staticmethod
    def _create_taxonomy(tx, level, taxonomy_query_dict):
        """
        Creates a node that represents a taxonomic label.
        :param tx: Neo4j transaction
        :param level: Label used for taxonomy node
        :param taxonomy_query_dict: Dictionary of taxon labels
        :return:
        """
        query = "WITH $batch as batch " \
                "UNWIND batch as record " \
                "MERGE (a:" + level + " {name:record.label}) RETURN a"
        _run_subbatch(tx, query, taxonomy_query_dict)

    @staticmethod
    def _connect_taxonomy(tx, level1, level2, taxonomy_query_dict):
        """
        Connects a taxonomic label to its higher-level label.
        :param tx: Neo4j transaction
        :param level1: Label used for lower-level taxonomy node
        :param level2: Label used for upper-level taxonomy node
        :param taxonomy_query_dict: Dictionary of taxon label
        :return:
        """
        query = "WITH $batch as batch " \
                "UNWIND batch as record " \
                "MATCH (a:" + level1 + " {name:record.label1}), (b:" + level2 + \
                " {name:record.label2}) " \
                "MERGE (a)-[r:MEMBER_OF]->(b) RETURN type(r)"
        _run_subbatch(tx, query, taxonomy_query_dict)

    @staticmethod
    def _add_taxonomy(tx, level, taxonomy_query_dict):
        """
        Connects taxon node to taxonomy node.
        :param tx: Neo4j transaction
        :param level: Label used for taxonomy node
        :param taxonomy_query_dict: List of taxon IDs
        :return:
        """
        query = "WITH $batch as batch " \
                "UNWIND batch as record " \
                "MATCH (a:Taxon) " \
                "MATCH (b:" + level + ") " \
                "WHERE a.name = record.taxon AND b.name = record.level " \
                "MERGE (a)-[r:MEMBER_OF]->(b) RETURN type(r)"
        _run_subbatch(tx, query, taxonomy_query_dict)

    @staticmethod
    def _create_sample(tx, sample_query_dict):
        """
        Creates sample nodes and link to experiment.
        :param tx: Neo4j transaction
        :param sample_query_dict: List of dictionaries with sample names and experiment IDs
        :param exp_id: Experiment name
        :return:
        """
        query = "WITH $batch as batch " \
                "UNWIND batch as record " \
                "MERGE (a:Specimen {name:record.sample}) RETURN a"
        _run_subbatch(tx, query, sample_query_dict)
        query = "WITH $batch as batch " \
                "UNWIND batch as record " \
                "MATCH (a:Specimen {name:record.sample}), (b:Experiment {name:record.exp_id}) " \
                "MERGE (a)-[r:PART_OF]->(b) RETURN type(r)"
        _run_subbatch(tx, query, sample_query_dict)

    @staticmethod
    def _create_property(tx, property_query_dict):
        """
        Creates target node if it does not exist yet
        and adds the relationship between target and source.
        :param tx: Neo4j transaction
        :param property_query_dict: List of dictionaries with property names
        :return:
        """
        query = "WITH $batch as batch " \
                "UNWIND batch as record " \
                "MERGE (a:Property {name:record.label}) RETURN a"
        _run_subbatch(tx, query, property_query_dict)

    @staticmethod
    def _connect_property(tx, property_query_dict, sourcetype=''):
        """
        Creates target node if it does not exist yet
        and adds the relationship between target and source.
        :param tx: Neo4j transaction
        :param property_query_dict: List of dictionaries with property names, targets, values of relationship.
        :return:
        """
        if len(sourcetype) > 0:
            sourcetype = ':' + sourcetype
        rel = ""
        weight_rel = None
        val_rel = None
        if 'weight' in property_query_dict[0]:
            weight_rel = "weight: record.weight"
        if 'value' in property_query_dict[0]:
            val_rel = "value: record.value"
        if weight_rel and val_rel:
            rel = " {" + weight_rel + ", " + val_rel + "}"
        elif weight_rel:
            rel = " {" + weight_rel + "}"
        elif val_rel:
            rel = " {" + val_rel + "}"
        query = "WITH $batch as batch " \
                "UNWIND batch as record " \
                "MATCH (a" + sourcetype + " {name:record.source}), (b:Property {name:record.name}) " \
                "MERGE (a)-[r:QUALITY_OF" + rel + "]->(b) " \
                "RETURN type(r)"
        _run_subbatch(tx, query, property_query_dict)

    @staticmethod
    def _create_observations(tx, observations):
        """
        Creates relationships between taxa and samples
        that represent the count number of that taxon in a sample.
        :param tx: Neo4j transaction
        :param observations: A list of dictionaries containing taxon name, sample ID and count.
        :return:
        """
        query = "WITH $batch as batch " \
                "UNWIND batch as record " \
                "MATCH (a:Taxon {name: record.taxon}), (b:Specimen {name: record.sample}) " \
                "MERGE (a)-[r:LOCATED_IN {count: record.value}]->(b) " \
                "RETURN type(r)"
        _run_subbatch(tx, query, observations)

    @staticmethod
    def _samples_to_delete(tx, exp_id):
        """
        Generates a list of sample nodes linked to the experiment node that needs to be deleted.
        :param tx:
        :param exp_id: ID of experiment node
        :return:
        """
        names = tx.run(("MATCH (a:Specimen)-[r]-(b:Experiment) "
                        "WHERE b.name = '" + exp_id +
                        "' RETURN a.name")).data()
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
        names = tx.run("MATCH (a:Taxon) WHERE NOT (a)--(:Specimen) RETURN a.name").data()
        return names

    @staticmethod
    def _delete_sample(tx, deletion_dict):
        """
        Deletes a sample node and all the observations linked to the sample.
        Only samples present in a single experiment are deleted.
        :param tx:
        :param deletion_dict: List of dictionaries containing sample + experiment ID
        :return:
        """
        query = "WITH $batch as batch " \
                "UNWIND batch as record " \
                "MATCH (a:Specimen {name:record.sample})--(b:Experiment {name:record.exp_id}) " \
                "DETACH DELETE a"
        _run_subbatch(tx, query, deletion_dict)

    @staticmethod
    def _delete_taxon(tx, deletion_dict):
        """
        Deletes a taxon and all the edges linked to the taxon.
        :param tx:
        :param deletion_dict: List of dictionaries containing taxon identifiers
        :return:
        """
        query = "WITH $batch as batch " \
                "UNWIND batch as record " \
                "MATCH (a:Taxon {name:record.taxon})--(b:Edge) " \
                "DETACH DELETE b"
        _run_subbatch(tx, query, deletion_dict)
        query = "WITH $batch as batch " \
                "UNWIND batch as record " \
                "MATCH (a:Taxon {name:record.taxon}) " \
                "DETACH DELETE a"
        _run_subbatch(tx, query, deletion_dict)

    @staticmethod
    def _create_indices(tx):
        """
        (Re)creates indices for specimen, taxon and property nodes.
        This speeds up queries that connect such nodes.
        :param tx:
        :return:
        """
        constraints = tx.run("CALL db.indexes() YIELD labelsOrTypes, properties "
                             "RETURN labelsOrTypes, properties").data()
        constraint_tuples = list()
        for val in constraints:
            if len(val['labelsOrTypes']) > 0 and 'name' in val['properties']:
                constraint_tuples.append((val['labelsOrTypes'][0],
                                      'name'))
        if ('Property', 'name') in constraint_tuples:
            tx.run("DROP INDEX on :Property(name)")
        if ('Specimen', 'name') in constraint_tuples:
            tx.run("DROP INDEX on :Specimen(name)")
        if ('Taxon', 'name') in constraint_tuples:
            tx.run("DROP INDEX on :Taxon(name)")
        tx.run("CREATE INDEX on :Property(name)")
        tx.run("CREATE INDEX on :Specimen(name)")
        tx.run("CREATE INDEX on :Taxon(name)")
