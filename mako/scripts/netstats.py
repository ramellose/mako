"""

"""

__author__ = 'Lisa Rottjers'
__maintainer__ = 'Lisa Rottjers'
__email__ = 'lisa.rottjers@kuleuven.be'
__status__ = 'Development'
__license__ = 'Apache 2.0'

import os
import sys
from neo4j.v1 import GraphDatabase
from mako.scripts.utils import _get_unique, _create_logger, _read_config
import logging.handlers

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
logpath = "\\".join(os.getcwd().split("\\")[:-1]) + '\\manta.log'
# filelog path is one folder above manta
# pyinstaller creates a temporary folder, so log would be deleted
fh = logging.handlers.RotatingFileHandler(maxBytes=500,
                                          filename=logpath, mode='a')
fh.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
logger.addHandler(fh)


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
        else:
            networks = inputs['networks']
        if 'union' in inputs['logic']:
            driver.graph_union(networks=networks)
        if 'intersection' in inputs['logic']:
            for n in inputs['fraction']:
                driver.graph_intersection(networks=networks,
                                          weight=inputs['weight'], n=int(n))
            if 'difference' in inputs['logic']:
                netdriver.graph_difference(networks=networks,
                                           weight=inputs['weight'])
            if inputs['networks'] is not None:
                names = [x.split('.')[0] for x in inputs['networks']]
                importdriver.export_network(path=inputs['fp'] + '/' +
                                            "_".join(names) + '.graphml')
                logger.info("Exporting networks to: " + inputs['fp'] + '/' +
                            "_".join(names) + '.graphml')
                checks += "Exporting networks to: " + inputs['fp'] + '/' +\
                          "_".join(names) + '.graphml' "\n"
            else:
                importdriver.export_network(path=inputs['fp'] + '/' +
                                                  '_complete.graphml')
                logger.info("Exporting networks to: " + inputs['fp'] + '/' +
                            '_complete.graphml')
                checks += "Exporting networks to: " + inputs['fp'] + '/' +\
                          '_complete.graphml' "\n"
        else:
            logger.warning("No logic operation specified!")
        if publish:
            pub.sendMessage('update', msg="Completed database operations!")
        # sys.stdout.write("Completed database operations!")
        checks += 'Completed database operations! \n'
    except Exception:
        logger.warning("Failed to run database worker.  ", exc_info=True)
        checks += 'Failed to run database worker. \n'
    if publish:
        pub.sendMessage('database_log', msg=checks)
    importdriver.close()
    netdriver.close()
    logger.info('Completed netstats operations!  ')

class NetstatsDriver(object):

    def __init__(self, uri, user, password, filepath):
        """
        Initializes a driver for accessing the Neo4j database.
        This driver extracts nodes and edges from the database that are required
        for the operations defined in the netstats module.

        :param uri: Adress of Neo4j database
        :param user: Username for Neo4j database
        :param password: Password for Neo4j database
        :param filepath: Filepath where logs will be written.
        """
        _create_logger(filepath)
        try:
            self._driver = GraphDatabase.driver(uri, auth=(user, password))
        except Exception:
            logger.error("Unable to start IoDriver. \n", exc_info=True)
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
