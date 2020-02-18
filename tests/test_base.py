"""
This file contains functions for testing functions in the base.py script.

The file first sets up a simple Neo4j database for carrying out the tests.

"""

import unittest
import os
from mako.scripts.base import start_base, BaseDriver
from mako.scripts.utils import _resource_path

__author__ = 'Lisa Rottjers'
__maintainer__ = 'Lisa Rottjers'
__email__ = 'lisa.rottjers@kuleuven.be'
__status__ = 'Development'
__license__ = 'Apache 2.0'


# since we do not want to overwrite the local Neo4j instance,
# we can run a container with Neo4j
# the command below starts a container named Neo4j
# that runs the latest version of Neo4j
# Accessible on http ports + 1
# so both local database and this can be run side by side
loc = os.environ['HOMEDRIVE'] + os.environ['HOMEPATH']
docker = "docker run \
--rm \
-d \
--publish=7475:7474 --publish=7688:7687 \
--name=neo4j \
--env NEO4J_AUTH=neo4j/test \
neo4j:latest"


class TestBase(unittest.TestCase):
    """
    Tests Base methods.
    """

    def test_start_base_pid(self):
        """
        Checks if an error is returned when the pid is wrong.

        :param inputs: Default arguments
        :return:
        """
        inputs = {'fp': 'C:/Users/u0118219/Documents/mako_files',
                  'neo4j': 'C://Users//u0118219//Documents//neo4j',
                  'username': 'neo4j',
                  'password': 'test',
                  'address': 'bolt://localhost:7688',
                  'start': False,
                  'clear': False,
                  'quit': False,
                  'store_config': True,
                  'check': False}
        print(_resource_path('config'))
        self.assertWarns(start_base(inputs))


if __name__ == '__main__':
    unittest.main()