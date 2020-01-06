"""
While users can set up their own (secure) Neo4j database, _mako_ can also set up  a local instance.
This file contains all functions necessary to start this instance,
as well as utility functions for interacting with the Neo4j database.

"""

__author__ = 'Lisa Rottjers'
__maintainer__ = 'Lisa Rottjers'
__email__ = 'lisa.rottjers@kuleuven.be'
__status__ = 'Development'
__license__ = 'Apache 2.0'

from neo4j.v1 import GraphDatabase
from uuid import uuid4  # generates unique IDs for associations + observations
import networkx as nx
from mako.scripts.utils import _get_unique, _create_logger
import numpy as np
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


class BaseDriver(object):

    def __init__(self, uri, user, password, filepath):
        """
        Initializes a driver for accessing the Neo4j database.
        This driver constructs the Neo4j database and uploads extra data.
        :param uri: Adress of Neo4j database
        :param user: Username for Neo4j database
        :param password: Password for Neo4j database
        :param filepath: Filepath where logs will be written.
        """
        _create_logger(filepath)
        try:
            self._driver = GraphDatabase.driver(uri, auth=(user, password))
        except Exception:
            logger.error("Unable to start BaseDriver. \n", exc_info=True)
            sys.exit()

    def close(self):
        """
        Closes the connection to the database.
        :return:
        """
        self._driver.close()

    def clear_database(self):
        """
        Clears the entire database.
        :return:
        """
        try:
            with self._driver.session() as session:
                session.write_transaction(self._delete_all)
        except Exception:
            logger.error("Could not clear database. \n", exc_info=True)

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

    def delete_experiment(self, exp_id):
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
    def _delete_all(tx):
        """
        Deletes all nodes and their relationships from the database.
        :param tx: Neo4j transaction
        :return:
        """
        tx.run("MATCH (n) DETACH DELETE n")

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







