"""
This file contains functions for testing functions in the neo4biom.py script.

The file first sets up a simple Neo4j database for carrying out the tests.

"""

import unittest
import time
import os
import biom
import networkx as nx
from mako.scripts.neo4biom import Biom2Neo
from mako.scripts.io import IoDriver
from mako.scripts.utils import _resource_path, _get_unique, _read_config, _get_path, ParentDriver

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


testraw = """{
     "id":  "test",
     "format": "Biological Observation Matrix 1.0.0-dev",
     "format_url": "http://biom-format.org",
     "type": "OTU table",
     "generated_by": "QIIME revision XYZ",
     "date": "2011-12-19T19:00:00",
     "rows":[
        {"id":"GG_OTU_1", "metadata":{"taxonomy":["k__Bacteria", "p__Proteoba\
cteria", "c__Gammaproteobacteria", "o__Enterobacteriales", "f__Enterobacteriac\
eae", "g__Escherichia", "s__"]}},
        {"id":"GG_OTU_2", "metadata":{"taxonomy":["k__Bacteria", "p__Cyanobact\
eria", "c__Nostocophycideae", "o__Nostocales", "f__Nostocaceae", "g__Dolichosp\
ermum", "s__"]}},
        {"id":"GG_OTU_3", "metadata":{"taxonomy":["k__Archaea", "p__Euryarchae\
ota", "c__Methanomicrobia", "o__Methanosarcinales", "f__Methanosarcinaceae", "\
g__Methanosarcina", "s__"]}},
        {"id":"GG_OTU_4", "metadata":{"taxonomy":["k__Bacteria", "p__Firmicute\
s", "c__Clostridia", "o__Halanaerobiales", "f__Halanaerobiaceae", "g__Halanaer\
obium", "s__Halanaerobiumsaccharolyticum"]}},
        {"id":"GG_OTU_5", "metadata":{"taxonomy":["k__Bacteria", "p__Proteobac\
teria", "c__Gammaproteobacteria", "o__Enterobacteriales", "f__Enterobacteriace\
ae", "g__Escherichia", "s__"]}}
        ],
     "columns":[
        {"id":"Sample1", "metadata":{
                                "BarcodeSequence":"CGCTTATCGAGA",
                                "LinkerPrimerSequence":"CATGCTGCCTCCCGTAGGAGT",
                                "BODY_SITE":"gut",
                                "Description":"human gut"}},
        {"id":"Sample2", "metadata":{
                                "BarcodeSequence":"CATACCAGTAGC",
                                "LinkerPrimerSequence":"CATGCTGCCTCCCGTAGGAGT",
                                "BODY_SITE":"gut",
                                "Description":"human gut"}},
        {"id":"Sample3", "metadata":{
                                "BarcodeSequence":"CTCTCTACCTGT",
                                "LinkerPrimerSequence":"CATGCTGCCTCCCGTAGGAGT",
                                "BODY_SITE":"gut",
                                "Description":"human gut"}},
        {"id":"Sample4", "metadata":{
                                "BarcodeSequence":"CTCTCGGCCTGT",
                                "LinkerPrimerSequence":"CATGCTGCCTCCCGTAGGAGT",
                                "BODY_SITE":"skin",
                                "Description":"human skin"}},
        {"id":"Sample5", "metadata":{
                                "BarcodeSequence":"CTCTCTACCAAT",
                                "LinkerPrimerSequence":"CATGCTGCCTCCCGTAGGAGT",
                                "BODY_SITE":"skin",
                                "Description":"human skin"}},
        {"id":"Sample6", "metadata":{
                                "BarcodeSequence":"CTAACTACCAAT",
                                "LinkerPrimerSequence":"CATGCTGCCTCCCGTAGGAGT",
                                "BODY_SITE":"skin",
                                "Description":"human skin"}}
        ],
     "matrix_type": "sparse",
     "matrix_element_type": "int",
     "shape": [5, 6],
     "data":[[0,2,1],
             [1,0,5],
             [1,1,1],
             [1,3,2],
             [1,4,3],
             [1,5,1],
             [2,2,1],
             [2,3,4],
             [2,5,2],
             [3,0,2],
             [3,1,1],
             [3,2,1],
             [3,5,1],
             [4,1,1],
             [4,2,1]
            ]
    }
"""

testbiom = biom.parse.parse_biom_table(testraw)

# make toy network
g = nx.Graph()
nodes = ["GG_OTU_1", "GG_OTU_2", "GG_OTU_3", "GG_OTU_4", "GG_OTU_5"]
g.add_nodes_from(nodes)
g.add_edges_from([("GG_OTU_1", "GG_OTU_2"),
                  ("GG_OTU_2", "GG_OTU_5"), ("GG_OTU_3", "GG_OTU_4")])
g["GG_OTU_1"]["GG_OTU_2"]['weight'] = 1.0
g["GG_OTU_2"]["GG_OTU_5"]['weight'] = 1.0
g["GG_OTU_3"]["GG_OTU_4"]['weight'] = -1.0


class TestNeo4Biom(unittest.TestCase):
    """
    Tests Base methods.
    Warning: most of these functions are to start a local database.
    Therefore, the presence of the necessary local files is a prerequisite.
    """
    @classmethod
    def setUpClass(cls):
        os.system(docker_command)
        time.sleep(20)
        driver = Biom2Neo(user='neo4j',
                          password='test',
                          uri='bolt://localhost:7688', filepath=_resource_path(''),
                          encrypted=False)
        driver.convert_biom(testbiom, exp_id='test')
        driver = IoDriver(user='neo4j',
                          password='test',
                          uri='bolt://localhost:7688', filepath=_resource_path(''),
                          encrypted=False)
        driver.convert_networkx(network=g, network_id='g')

    @classmethod
    def tearDownClass(cls):
        driver = Biom2Neo(user='neo4j',
                          password='test',
                          uri='bolt://localhost:7688', filepath=_resource_path(''),
                          encrypted=False)
        driver.query('MATCH (n) DETACH DELETE n')
        os.system('docker stop neo4j')

    def test_get_unique(self):
        """
        Checks if unique nodes are extracted from the Neo4j query.
        :return:
        """
        driver = IoDriver(user='neo4j',
                          password='test',
                          uri='bolt://localhost:7688', filepath=_resource_path(''),
                          encrypted=False)
        test = driver.query("MATCH (n:Network)--() RETURN n")
        outcome = _get_unique(test, key='n')
        self.assertGreater(len(test), len(outcome))

    def test_read_config(self):
        """
        Checks if the config in the test location contains 5 keys.
        :return:
        """
        config = _read_config({'store_config': True})
        self.assertEqual(len(config), 5)

    def test_get_path(self):
        """
        Checks if the path function returns the complete file path.
        :return:
        """
        path = _get_path('test_utils.py', default=_resource_path(''))
        self.assertTrue(os.path.isfile(path))

    def test_get_path_false(self):
        """
        Checks if the path function returns False if the file does not exist
        in any of the specified locations (cwd, default).
        :return:
        """
        path = _get_path('netstats.py', default=_resource_path(''))
        self.assertFalse(os.path.isfile(path))

    def test_ParentDriver(self):
        """
        Checks if the ParentDriver can process a query.
        :return:
        """
        driver = ParentDriver(user='neo4j',
                              password='test',
                              uri='bolt://localhost:7688', filepath=_resource_path(''),
                              encrypted=False)
        driver.query("CREATE (n:Node {name: 'Test'}) RETURN n")
        test = driver.query("MATCH (n:Node {name: 'Test'}) RETURN n")
        self.assertEqual(len(test), 1)


if __name__ == '__main__':
    unittest.main()


