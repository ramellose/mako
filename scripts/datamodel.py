"""
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
import os
import sys
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


# the MAO file can be loaded from the MAO file
# the only changes that have been made to this owl file
# are the cardinalities of the specified relations
# e.g a species is part of 1 family, not 2 families
onto = owlready2.get_ontology(os.getcwd() + "\\MAO3.owl")
onto.load()

# since the MAO file is small, we can load the objects into lists
objects = list(onto.classes())
properties = list(onto.object_properties())

# replace all label spaces with underscores
for val in objects:
    val.label = [x.replace(" ", "_") for x in val.label]
for val in properties:
    val.label = [x.replace(" ", "_") for x in val.label]

# every property is an edge in the Neo4J graph
# therefore, we can check the range and domains


def check_domain_range(neo4jdriver):
    """
    This function uses the Neo4j driver and the ontology to check whether there
    are properties in the database that violate the domains
    and ranges specified in the ontology.

    :param neo4jdriver: A database driver as instantiated by neo4base.py.
    :return: Success message or log of ontology violations
    """
    error = False
    for prop in properties:
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
        count = neo4jdriver.query(query)
        if count:
            logger.error("Relationship " + rel + " is connected to the wrong nodes!")
            error = True
    if not error:
        logger.info("No forbidden relationship connections.")


def check_only(neo4jdriver):
    """
    This function uses the Neo4j driver and the ontology to check whether there
    are properties in the database that violate the 'only' axiom.

    :param neo4jdriver: A database driver as instantiated by neo4base.py.
    :return: Success message or log of ontology violations
    """
    error = False
    for prop in properties:
        rel = prop.label[0]

        neighbours = domains + ranges
        if len(neighbours) > 0:
            query = "MATCH (n)-[r:" + rel + "] WHERE NOT "
            for i in range(len(neighbours)):
                if i != 0:
                    query += (" AND NOT ")
                query += ("n:" + neighbours[i])
            query += (" RETURN count(n) as count")
        count = neo4jdriver.query(query)
        if count:
            logger.error("Relationship " + rel + " is connected to the wrong nodes!")
            error = True
    if not error:
        logger.info("No forbidden relationship connections.")
