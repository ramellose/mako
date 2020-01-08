"""
This file contains parsers and functions that call on other functionality defined
in the rest of massoc's scripts directory.
The command line interface is intended to be called sequentially;
files are written to disk as intermediates,
while a settings file is used to transfer logs and other information
between the modules. These modules are contained in this file.
This modular design allows users to leave out parts of massoc that are not required,
and reduces the number of parameters that need to be defined in the function calls.
"""

__author__ = 'Lisa Rottjers'
__maintainer__ = 'Lisa Rottjers'
__email__ = 'lisa.rottjers@kuleuven.be'
__status__ = 'Development'
__license__ = 'Apache 2.0'

import sys
import os
from platform import system
from subprocess import Popen
from psutil import Process, pid_exists
from time import sleep
import networkx as nx
from wx.lib.pubsub import pub
from mako.scripts.utils import _create_logger
from mako.scrips.base import start
import logging
import logging.handlers

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# handler to sys.stdout
sh = logging.StreamHandler(sys.stdout)
sh.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
sh.setFormatter(formatter)
logger.addHandler(sh)


def set_mako(args):
    """
    Main function for running mako.
    Defines the argument parser.

    :param args: Arguments.
    :return:
    """
    # handler to file
    # construct logger after filepath is provided
    if 'base' in args:
        logger.info('Running base Neo4j module. ')
        get_input(massoc_args)
    if 'network' in massoc_args:
        logger.info('Running network inference module. ')
        run_network(massoc_args)
    if 'database' in massoc_args:
        logger.info('Working on Neo4j database. ')
        run_neo4j(massoc_args)
    if 'netstats' in massoc_args:
        logger.info('Performing network analysis on Neo4j database. ')
        run_netstats(massoc_args)
    if 'metastats' in massoc_args:
        logger.info('Performing metadata analysis on Neo4j database. ')
        run_metastats(massoc_args)
    logger.info('Completed tasks! ')

def resource_path(relative_path):
    """
    Get absolute path to resource, works for dev and for PyInstaller.
    Source: https://stackoverflow.com/questions/7674790/bundling-data-files-with-pyinstaller-onefile
    :param relative_path: Path to MEI location.
    :return:
    """
    try:
        # PyInstaller creates a temp folder and stores path in _MEIPASS
        base_path = sys._MEIPASS
    except Exception:
        base_path = os.path.abspath(".")
    return os.path.join(base_path, relative_path)


def start_database(inputs, publish):
    """
    Starts Neo4j database.
    :param inputs: Dictionary of inputs.
    :param publish: If True, publishes messages to be received by GUI.
    :return:
    """
    try:
        if publish:
            pub.sendMessage('update', msg='Starting database...')
        if system() == 'Windows':
            filepath = inputs['neo4j'] + '/bin/neo4j.bat console'
        else:
            filepath = inputs['neo4j'] + '/bin/neo4j console'
        filepath = filepath.replace("\\", "/")
        if system() == 'Windows' or system() == 'Darwin':
            p = Popen(filepath, shell=True)
        else:
            # note: old version used gnome-terminal, worked with Ubuntu
            # new version uses xterm to work with macOS
            # check if this conflicts!
            p = Popen(["gnome-terminal", "-e", filepath])  # x-term compatible alternative terminal
        inputs['pid'] = p.pid
        if publish:
            pub.sendMessage('pid', msg=inputs['pid'])
        sleep(12)
    except Exception:
        logger.warning("Failed to start database.  ", exc_info=True)
