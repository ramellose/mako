"""
This file contains different utility functions necessary across other modules.
"""

__author__ = 'Lisa Rottjers'
__maintainer__ = 'Lisa Rottjers'
__email__ = 'lisa.rottjers@kuleuven.be'
__status__ = 'Development'
__license__ = 'Apache 2.0'

import logging
import logging.handlers
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
