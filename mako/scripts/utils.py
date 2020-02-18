"""
This file contains different utility functions necessary across other modules.
"""

__author__ = 'Lisa Rottjers'
__maintainer__ = 'Lisa Rottjers'
__email__ = 'lisa.rottjers@kuleuven.be'
__status__ = 'Development'
__license__ = 'Apache 2.0'

import sys
import os
import numpy as np
import logging
import logging.handlers
from neo4j.v1 import GraphDatabase
logger = logging.getLogger(__name__)


def _get_unique(node_list, key, mode=None):
    """
    Returns number or names of unique nodes in a list of dictionaries.
    :param node_list: List of dictionaries returned by Neo4j transactions.
    :param key: Key accessing specific node in dictionary.
    :param mode: If 'num', the number of unique nodes is returned.
    :return: Unique nodes (list of nodes) or node number
    """
    unique_samples = list()
    for item in node_list:
        unique_samples.append(item[key].get('name'))
    unique_samples = set(unique_samples)
    if mode == 'num':
        unique_samples = len(unique_samples)
    return unique_samples


def _create_logger(filepath):
    """
    After a filepath has become available, loggers can be created
    when required to report on errors.
    :param filepath: Filepath where logs will be written.
    :return:
    """
    logpath = filepath + '/massoc.log'
    # filelog path is one folder above massoc
    # pyinstaller creates a temporary folder, so log would be deleted
    fh = logging.handlers.RotatingFileHandler(maxBytes=500,
                                              filename=logpath, mode='a')
    fh.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    logger.addHandler(fh)


def _resource_path(relative_path):
    """
    Get absolute path to resource, works for dev and for PyInstaller.
    Source: https://stackoverflow.com/questions/7674790/bundling-data-files-with-pyinstaller-onefile
    :param relative_path: Path to MEI location.
    :return:
    """
    try:
        # PyInstaller creates a temp folder and stores path in _MEIPASS
        base_path = sys._MEIPASS
        if base_path[-5:] == 'tests':
            base_path = base_path[-6:]
    except Exception:
        base_path = os.path.abspath(".")
    return os.path.join(base_path, relative_path)


def _read_config(args):
    """
    Reads the mako config file to preload Neo4j access settings.
    If the arguments are specified, this overwrites the config file.
    After reading the config file and parsing the arguments,
    no value should be zero.

    :param args: User-supplied arguments
    :param store_config: If True, Neo4j credentials are stored.
    :return: Neo4j credentials
    """
    config = dict()
    with open(_resource_path('config'), 'r') as file:
        # read a list of lines into data
        configfile = file.readlines()
    for line in configfile[2:]:
        key = line.split(':')[0]
        val = line.split(' ')[-1].strip()
        config[key] = val
    for key in set(config.keys()).intersection(args.keys()):
        if args[key]:
            config[key] = args[key]
        if config[key] == 'None':
            logger.error('Could not read login information from config or from arguments. \n')
    with open(_resource_path('config'), 'w') as file:
        newlines = configfile[:3]
        if args['store_config']:
            for line in configfile[3:]:
                key = line.split(':')[0]
                newline = key + ': ' + config[key] + '\n'
                newlines.append(newline)
        else:
            for line in configfile[3:]:
                key = line.split(':')[0]
                newline = key + ': None' + '\n'
                newlines.append(newline)
        file.writelines(newlines)
    return config


def _get_path(path, default):
    """
    If given a path that is not a directory,
    this function checks if the file exists,
    if it is in the current working directory
    or if it is in the default path.

    If it cannot find the file, it raises an error.

    :param path: Partial or complete file path
    :param default: default file path
    :return:
    """
    checked_path = False
    if os.path.isfile(path):
        checked_path = path
    elif os.path.isfile(os.getcwd() + '/' + path):
        checked_path = os.getcwd() + '/' + path
    elif os.path.isfile(default + '/' + path):
        checked_path = default + '/' + path
    else:
        logger.error('Unable to import ' + path + '!\n', exc_info=True)
    return checked_path


class ParentDriver:
    def __init__(self, uri, user, password, filepath):
        """
        Initializes a driver for accessing the Neo4j database.

        :param uri: Adress of Neo4j database
        :param user: Username for Neo4j database
        :param password: Password for Neo4j database
        :param filepath: Filepath where logs will be written.
        """
        _create_logger(filepath)
        try:
            self._driver = GraphDatabase.driver(uri, auth=(user, password))
        except Exception:
            logger.error("Unable to start driver. \n", exc_info=True)
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


