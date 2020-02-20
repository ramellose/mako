"""
This file contains functions for testing functions in the base.py script.

The file first sets up a simple Neo4j database for carrying out the tests.

"""

import unittest
import os
import time
from psutil import pid_exists, Process
from signal import CTRL_C_EVENT
from mako.scripts.base import start_base, BaseDriver
from mako.scripts.utils import _read_config, _resource_path

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
docker_command = "docker run \
--rm \
-d \
--publish=7475:7474 --publish=7688:7687 \
--name=neo4j \
--env NEO4J_AUTH=neo4j/test \
neo4j:latest"


class TestBase(unittest.TestCase):
    """
    Tests Base methods.
    Warning: most of these functions are to start a local database.
    Therefore, the presence of the necessary local files is a prerequisite.
    """
    @classmethod
    def setUpClass(cls):
        os.system(docker_command)
        time.sleep(20)

    @classmethod
    def tearDownClass(cls):
        os.system('docker stop neo4j')

    def test_start_base_pid(self):
        """
        Checks if an error is returned when the pid is wrong.

        :return:
        """
        inputs = {'fp': _resource_path(''),
                  'neo4j': loc + '//Documents//neo4j',
                  'username': 'neo4j',
                  'password': 'test',
                  'address': 'bolt://localhost:7687',
                  'start': True,
                  'clear': False,
                  'quit': False,
                  'store_config': True,
                  'check': False}
        start_base(inputs)
        config = _read_config(inputs)
        self.assertTrue(pid_exists(int(config['pid'])))
        parent = Process(int(config['pid']))
        parent.send_signal(CTRL_C_EVENT)

    def test_stop_base_pid(self):
        """
        Checks if an error is returned when the pid is wrong.

        :return:
        """
        inputs = {'fp': _resource_path(''),
                  'neo4j': loc + 'Documents//neo4j',
                  'username': 'neo4j',
                  'password': 'test',
                  'address': 'bolt://localhost:7687',
                  'start': True,
                  'clear': False,
                  'quit': False,
                  'store_config': False,
                  'check': False}
        start_base(inputs)
        inputs = {'fp': _resource_path(''),
                  'neo4j': loc + 'Documents//neo4j',
                  'username': 'neo4j',
                  'password': 'test',
                  'address': 'bolt://localhost:7687',
                  'start': False,
                  'clear': False,
                  'quit': True,
                  'store_config': False,
                  'check': False}
        start_base(inputs)
        config = _read_config(inputs)
        self.assertFalse(pid_exists(int(config['pid'])))

    def test_clear_database(self):
        """
        First writes a single node to the Docker Neo4j database and then clears it.
        :return:
        """
        inputs = {'fp': _resource_path(''),
                  'neo4j': loc + 'Documents//neo4j',
                  'username': 'neo4j',
                  'password': 'test',
                  'address': 'bolt://localhost:7688',
                  'start': False,
                  'clear': True,
                  'quit': False,
                  'store_config': False,
                  'check': False,
                  'encryption': False}
        driver = BaseDriver(user=inputs['username'],
                            password=inputs['password'],
                            uri=inputs['address'], filepath=inputs['fp'],
                            encrypted=False)
        driver.query("CREATE (n:Test) RETURN n")
        start_base(inputs)
        result = driver.query("MATCH (n) RETURN count(n)")
        self.assertEqual(result[0]['count(n)'], 0)

    def test_check_database_correct(self):
        """
        Checks if the check database returns True when there are no wrong connections.
        :return:
        """
        inputs = {'fp': _resource_path(''),
                  'neo4j': loc + 'Documents//neo4j',
                  'username': 'neo4j',
                  'password': 'test',
                  'address': 'bolt://localhost:7688',
                  'start': False,
                  'clear': True,
                  'quit': False,
                  'store_config': False,
                  'check': False,
                  'encryption': False}
        start_base(inputs)
        driver = BaseDriver(user=inputs['username'],
                            password=inputs['password'],
                            uri=inputs['address'], filepath=inputs['fp'],
                            encrypted=False)
        driver.query("CREATE (n:Edge {name: 'edge'}) RETURN n")
        driver.query("CREATE (n:Network {name: 'network'}) RETURN n")
        driver.query("CREATE (n:Specimen {name: 'specimen'}) RETURN n")
        driver.query("MATCH (n:Edge {name: 'edge'}), (m:Network {name: 'network'}) "
                     "CREATE (n)-[r:PART_OF]->(m) return type(r)")
        outcome = driver.check_domain_range()
        start_base(inputs)
        self.assertFalse(outcome)

    def test_check_database_wrong(self):
        """
        Checks if the check database returns True when there are no wrong connections.
        :return:
        """
        inputs = {'fp': _resource_path(''),
                  'neo4j': loc + 'Documents//neo4j',
                  'username': 'neo4j',
                  'password': 'test',
                  'address': 'bolt://localhost:7688',
                  'start': False,
                  'clear': True,
                  'quit': False,
                  'store_config': False,
                  'check': False,
                  'encryption': False}
        start_base(inputs)
        driver = BaseDriver(user=inputs['username'],
                            password=inputs['password'],
                            uri=inputs['address'], filepath=inputs['fp'],
                            encrypted=False)
        driver.query("CREATE (n:Edge {name: 'edge'}) RETURN n")
        driver.query("CREATE (n:Network {name: 'network'}) RETURN n")
        driver.query("CREATE (n:Genus {name: 'genus'}) RETURN n")
        driver.query("MATCH (n:Genus {name: 'genus'}), (m:Network {name: 'network'}) "
                     "CREATE (n)-[r:PART_OF]->(m) return type(r)")
        outcome = driver.check_domain_range()
        start_base(inputs)
        self.assertTrue(outcome)


if __name__ == '__main__':
    unittest.main()

