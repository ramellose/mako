"""
While users can set up their own (secure) Neo4j database, _mako_ can also set up  a local instance.
This file contains all functions necessary to start this instance,
as well as utility functions for interacting with the Neo4j database.

The database model for biological data is arguably the core component of _mako_.
This file contains functions for validating the database model;
any violations are reported to the user.

The data model is defined as a set of checks for the Neo4j graph.
Neo4j does not support structural constraints, so instead, we use these to check.
"""

__author__ = 'Lisa Rottjers'
__maintainer__ = 'Lisa Rottjers'
__email__ = 'lisa.rottjers@kuleuven.be'
__status__ = 'Development'
__license__ = 'Apache 2.0'

import owlready2
from neo4j.v1 import GraphDatabase
from mako.scripts.utils import ParentDriver, _create_logger, _resource_path, _read_config
import logging
import sys
import os
from platform import system
from subprocess import Popen
from psutil import Process, pid_exists
from signal import CTRL_C_EVENT
from time import sleep

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# handler to sys.stdout
sh = logging.StreamHandler(sys.stdout)
sh.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
sh.setFormatter(formatter)
logger.addHandler(sh)


def start_base(inputs):
    """

    :param inputs: Dictionary of arguments.
    :return:
    """
    _create_logger(inputs['fp'])
    # check if pid exists from previous session
    config = _read_config(inputs)
    pid = config['pid']
    if pid != 'None':
        pid = int(pid)
        if not pid_exists(pid):
            logger.warning('The PID file is incorrect. Resetting PID file. \n'
                           'This is usually the result of a forced shutdown. \n'
                           'Please use "mako base quit" to shut down safely. \n')
            with open(_resource_path('config'), 'r') as file:
                # read a list of lines into data
                data = file.readlines()
            with open(_resource_path('config'), 'w') as file:
                data[2] = 'pid: None' + '\n'
                file.writelines(data)
            pid = 'None'
    if inputs['start'] and pid == 'None':
        try:
            if system() == 'Windows':
                filepath = inputs['neo4j'] + '/bin/neo4j.bat console'
            else:
                filepath = inputs['neo4j'] + '/bin/neo4j console'
            filepath = filepath.replace("\\", "/")
            if system() == 'Windows' or system() == 'Darwin':
                p = Popen(filepath, shell=False)
            else:
                # note: old version used gnome-terminal, worked with Ubuntu
                # new version uses xterm to work with macOS
                # check if this conflicts!
                p = Popen(["gnome-terminal", "-e", filepath])  # x-term compatible alternative terminal
            with open(_resource_path('config'), 'r') as file:
                # read a list of lines into data
                data = file.readlines()
            with open(_resource_path('config'), 'w') as file:
                data[2] = 'pid: ' + str(p.pid) + '\n'
                pid = p.pid
                file.writelines(data)
            sleep(12)
            driver = BaseDriver(user=config['username'],
                                password=config['password'],
                                uri=config['address'], filepath=inputs['fp'])
            driver.add_constraints()
            driver.close()
        except Exception:
            logger.warning("Failed to start database.  ", exc_info=True)
            exit()
    if inputs['clear'] and pid_exists(pid):
        driver = BaseDriver(user=config['username'],
                            password=config['password'],
                            uri=config['address'], filepath=inputs['fp'])
        try:
            driver.clear_database()
            logger.info('Cleared database.')
        except Exception:
            logger.warning("Failed to clear database.  ", exc_info=True)
    if inputs['quit'] and pid_exists(pid):
        try:
            with open(_resource_path('config'), 'r') as file:
                # read a list of lines into data
                data = file.readlines()
            with open(_resource_path('config'), 'w') as file:
                data[2] = 'pid: None'+ '\n'
                file.writelines(data)
            parent = Process(pid)
            parent.send_signal(CTRL_C_EVENT)
            # kills the powershell started to run the Neo4j console
            for child in parent.children():
                child.kill()
            logger.info('Safely shut down database.')
        except Exception:
            logger.warning("Failed to close database. ", exc_info=True)
    if inputs['check'] and pid_exists(pid):
        driver = BaseDriver(user=config['username'],
                            password=config['password'],
                            uri=config['address'], filepath=inputs['fp'])
        try:
            driver.check_domain_range()
        except Exception:
            logger.warning("Failed to check database.  ", exc_info=True)
    logger.info('Completed database operations!  ')


class BaseDriver(ParentDriver):
    """
    Initializes a driver for accessing the Neo4j database.
    This driver constructs the Neo4j database and uploads extra data.
    """
    def __init__(self, uri, user, password, filepath):
        """
        Overwrites parent driver to add ontology.

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
        # load the ontology that defines the schema
        # load the ontology that defines the schema
        onto = owlready2.get_ontology(os.getcwd() + "\\MAO.owl")
        onto.load()

        # since the MAO file is small, we can load the objects into lists
        self.objects = list(onto.classes())
        self.properties = list(onto.object_properties())

        # replace all label spaces with underscores
        for val in self.objects:
            val.label = [x.replace(" ", "_") for x in val.label]
        for val in self.properties:
            val.label = [x.replace(" ", "_") for x in val.label]

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

    def check_domain_range(self):
        """
        This function uses the Neo4j driver and the ontology to check whether there
        are properties in the database that violate the domains
        and ranges specified in the ontology.

        :param neo4jdriver: A database driver as instantiated by base.py.
        :return: Success message or log of ontology violations
        """
        error = False
        for prop in self.properties:
            rel = prop.label[0]
            domains = [x.label[0] for x in prop.get_domain()]
            ranges = [x.label[0] for x in prop.get_range()]
            neighbours = domains + ranges
            if len(neighbours) > 0:
                query = "MATCH (n)-[r:" + rel + "] WHERE NOT "
                for i in range(len(neighbours)):
                    if i != 0:
                        query += (" AND NOT ")
                    query += ("n:" + neighbours[i])
                query += (" RETURN count(n) as count")
            count = self.query(query)
            if count:
                logger.error("Relationship " + rel + " is connected to the wrong nodes!")
                error = True
        if not error:
            logger.info("No forbidden relationship connections.")

    def add_constraints(self):
        """
        This function adds some constraints for unique node names.

        :return:
        """
        unique_names = ['Edge', 'Class', 'Experiment', 'Family', 'Genus',
                        'Kingdom', 'Network', 'Order', 'Phylum',
                        'Property', 'Sample', 'Taxon']
        for name in unique_names:
            constraintname = 'Unique ' + name
            constraint = "CREATE CONSTRAINT ON (n:" + name + ")" \
                         " ASSERT (n.name) IS UNIQUE"
            with self._driver.session() as session:
                output = session.read_transaction(self._query, constraint)

    def check_only(self):
        """
        This function uses the Neo4j driver and the ontology to check whether there
        are properties in the database that violate the 'only' axiom.

        :param neo4jdriver: A database driver as instantiated by base.py.
        :return: Success message or log of ontology violations
        """
        pass
        # error = False
        # for prop in properties:
        # rel = prop.label[0]
        # if len(neighbours) > 0:
        #    query = "MATCH (n)-[r:" + rel + "] WHERE NOT "
        #    for i in range(len(neighbours)):
        #        if i != 0:
        #            query += (" AND NOT ")
        #        query += ("n:" + neighbours[i])
        #    query += (" RETURN count(n) as count")
        # count = neo4jdriver.query(query)
        # if count:
        #    logger.error("Relationship " + rel + " is connected to the wrong nodes!")
        #    error = True
        # if not error:
        # logger.info("No forbidden relationship connections.")

    @staticmethod
    def _delete_all(tx):
        """
        Deletes all nodes and their relationships from the database.
        :param tx: Neo4j transaction
        :return:
        """
        tx.run("MATCH (n) DETACH DELETE n")







