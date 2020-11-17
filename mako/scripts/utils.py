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
import wx
import wx.lib.newevent
import mako
import logging
import logging.handlers
from neo4j.v1 import GraphDatabase
logger = logging.getLogger(__name__)

wxLogEvent, EVT_WX_LOG_EVENT = wx.lib.newevent.NewEvent()


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
    logpath = filepath + '/mako.log'
    # filelog path is one folder above mako
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
        base_path = list(mako.__path__)[0]
    except Exception:
        base_path = sys._MEIPASS
    # if running the GUI, we need to set the path one dir higher
    splitpath = base_path.split(sep='\\')
    if splitpath[-1] == splitpath[-2]:
        base_path = os.path.abspath(base_path + "\\..")
    return os.path.join(base_path, relative_path)


def _read_config(args):
    """
    Reads the mako config file to preload Neo4j access settings.
    If the arguments are specified, this overwrites the config file.
    After reading the config file and parsing the arguments,
    no value should be zero.

    :param args: User-supplied arguments
    :return: Neo4j credentials
    """
    config = dict()
    try:
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
        for key in config:
            # update with args values
            if args[key]:
                config[key] = args[key]
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
    except FileNotFoundError:
        config = {'pid': 'None',
                  'address': 'None',
                  'password': 'None',
                  'username': 'None'}
    return config


def query(args, query):
    """
    Exports Neo4j query as logger info.

    :param args: User-supplied arguments as dict
    :param query: Cypher query as string
    :param result: Empty result query
    :return: None
    """
    driver = ParentDriver(uri=args['address'],
                          user=args['username'],
                          password=args['password'],
                          filepath=_resource_path(''))
    result = driver.query(query)
    logger.info(result)
    driver.close()
    return result


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


def _run_subbatch(tx, query, query_dict):
    """
    Batch queries can get so big that they cause memory issues.
    This function splits up the batches so this behaviour is avoided.
    While the apoc.periodic.commit could also fix this,
    the apoc JAR needs to be loaded first.

    :param tx: Neo4j handler
    :param query: String query to run
    :param query_dict: List of dictonaries that needs to be split to run well
    :return:
    """
    for i in range(0, len(query_dict), 100):
        if i + 100 > len(query_dict):
            subdict = query_dict[i:len(query_dict) - 1]
        else:
            subdict = query_dict[i:i + 100]
        tx.run(query, batch=subdict)

class ParentDriver:
    def __init__(self, uri, user, password, filepath, encrypted=True):
        """
        Initializes a driver for accessing the Neo4j database.

        :param uri: Adress of Neo4j database
        :param user: Username for Neo4j database
        :param password: Password for Neo4j database
        :param filepath: Filepath where logs will be written.
        :param encrypted: Can be set to False to interact with Docker during testing
        """
        _create_logger(filepath)
        try:
            self._driver = GraphDatabase.driver(uri, auth=(user, password), encrypted=encrypted)
        except Exception:
            logger.error("Unable to start driver. \n", exc_info=True)
            sys.exit()

    def close(self):
        """
        Closes the connection to the database.
        :return:
        """
        self._driver.close()

    def query(self, query, batch=None):
        """
        Accepts a query and provides the results.
        For batch queries, the batch parameter should contain
        a list of dictionaries, where each dictionary contains
        a key: value pair where the key matches a key in the
        Cypher query.

        Batch queries should unwind the batch,
        like so:
                query = "WITH $batch as batch " \
                "UNWIND batch as record " \
                "MERGE (a:Taxon {name:record.taxon}) RETURN a"

        The string after record (here taxon)
        needs to match a dictionary key.

        :param query: String containing Cypher query
        :param batch: List of dictionaries for batch queries
        :return: Results of transaction with Cypher query
        """
        output = None
        try:
            with self._driver.session() as session:
                output = session.read_transaction(self._query, query, batch)
        except Exception:
            logger.error("Unable to execute query: " + query + '\n', exc_info=True)
        return output

    @staticmethod
    def _query(tx, query, batch=None):
        """
        Processes custom queries.
        :param tx: Neo4j transaction
        :param query: String of Cypher query
        :param batch: List of dictionaries for batch queries
        :return: Outcome of transaction
        """
        if batch:
            results = tx.run(query, batch=batch).data()
        else:
            results = tx.run(query).data()
        return results
