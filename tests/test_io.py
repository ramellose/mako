"""
This file contains functions for testing functions in the neo4biom.py script.

The file first sets up a simple Neo4j database for carrying out the tests.

"""

import unittest
import time
import os
import biom
import networkx as nx
import pandas as pd
from mako.scripts.neo4biom import Biom2Neo
from mako.scripts.io import start_io, IoDriver
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
        {"id":"GG_OTU_2", "metadata":{"taxonomy":["k__Bacteria", "p__Firmicute\
s", "c__Clostridia", "o__Halanaerobiales", "f__Halobacteroidaceae", "g__Sporoh\
alobacter", "s__lortetii"]}},
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
testdict = dict.fromkeys(testbiom._observation_ids)
testdict = {x: {'target': 'banana', 'weight': 1} for x in testdict}

# make toy network
g = nx.Graph()
nodes = ["GG_OTU_1", "GG_OTU_2", "GG_OTU_3", "GG_OTU_4", "GG_OTU_5"]
g.add_nodes_from(nodes)
g.add_edges_from([("GG_OTU_1", "GG_OTU_2"),
                  ("GG_OTU_2", "GG_OTU_5"), ("GG_OTU_3", "GG_OTU_4")])
g["GG_OTU_1"]["GG_OTU_2"]['weight'] = 1.0
g["GG_OTU_2"]["GG_OTU_5"]['weight'] = 1.0
g["GG_OTU_3"]["GG_OTU_4"]['weight'] = -1.0


class TestIo(unittest.TestCase):
    """
    Tests io methods.
    Warning: most of these functions are to start a local database.
    Therefore, the presence of the necessary local files is a prerequisite.
    """
    @classmethod
    def setUpClass(cls):
        os.system(docker_command)
        time.sleep(20)
        nx.write_graphml(g, _resource_path('test.graphml'))
        data = pd.DataFrame()
        data['Taxon'] = testdict.keys()
        data['Fruit'] = 'banana'
        data.to_csv(_resource_path('test.tsv'), sep='\t', index=False)

    @classmethod
    def tearDownClass(cls):
        os.system('docker stop neo4j')
        os.remove(_resource_path('test.graphml'))
        os.remove(_resource_path('test.tsv'))

    def test_start_io(self):
        """
        Checks if the network file is uploaded to the database.
        :return:
        """
        driver = Biom2Neo(user='neo4j',
                          password='test',
                          uri='bolt://localhost:7688', filepath=_resource_path(''),
                          encrypted=False)
        driver.convert_biom(testbiom, exp_id='test')
        inputs = {'networks': ['test.graphml'],
                  'fp': _resource_path(''),
                  'username': 'neo4j',
                  'password': 'test',
                  'address': 'bolt://localhost:7688',
                  'delete': None,
                  'store_config': False,
                  'cyto': None,
                  'fasta': None,
                  'meta': None,
                  'write': None,
                  'encryption': False}
        start_io(inputs)
        driver = IoDriver(user=inputs['username'],
                          password=inputs['password'],
                          uri=inputs['address'], filepath=inputs['fp'],
                          encrypted=False)
        test = driver.query("MATCH (n:Network) RETURN n")
        driver.query("MATCH (n) DETACH DELETE n")
        self.assertEqual(test[0]['n']['name'], 'test')

    def test_start_delete_network(self):
        """
        Checks if the network file
        is deleted.
        :return:
        """
        driver = Biom2Neo(user='neo4j',
                          password='test',
                          uri='bolt://localhost:7688', filepath=_resource_path(''),
                          encrypted=False)
        driver.convert_biom(testbiom, exp_id='test')
        inputs = {'networks': ['test.graphml'],
                  'fp': _resource_path(''),
                  'username': 'neo4j',
                  'password': 'test',
                  'address': 'bolt://localhost:7688',
                  'delete': None,
                  'store_config': False,
                  'cyto': None,
                  'fasta': None,
                  'meta': None,
                  'write': None,
                  'encryption': False}
        start_io(inputs)
        inputs = {'networks': None,
                  'fp': _resource_path(''),
                  'username': 'neo4j',
                  'password': 'test',
                  'address': 'bolt://localhost:7688',
                  'delete': ['test'],
                  'store_config': False,
                  'cyto': None,
                  'fasta': None,
                  'meta': None,
                  'write': None,
                  'encryption': False}
        start_io(inputs)
        driver = IoDriver(user=inputs['username'],
                          password=inputs['password'],
                          uri=inputs['address'], filepath=inputs['fp'],
                          encrypted=False)
        test = driver.query("MATCH (n:Network) RETURN n")
        driver.query("MATCH (n) DETACH DELETE n")
        self.assertEqual(len(test), 0)

    def test_delete_correct_network(self):
        """
        Checks if only the correct network file is deleted.
        :return:
        """
        driver = Biom2Neo(user='neo4j',
                          password='test',
                          uri='bolt://localhost:7688', filepath=_resource_path(''),
                          encrypted=False)
        driver.convert_biom(testbiom, exp_id='test')
        inputs = {'networks': ['test.graphml'],
                  'fp': _resource_path(''),
                  'username': 'neo4j',
                  'password': 'test',
                  'address': 'bolt://localhost:7688',
                  'delete': None,
                  'store_config': False,
                  'cyto': None,
                  'fasta': None,
                  'meta': None,
                  'write': None,
                  'encryption': False}
        driver = IoDriver(user=inputs['username'],
                          password=inputs['password'],
                          uri=inputs['address'], filepath=inputs['fp'],
                          encrypted=False)
        driver.convert_networkx(network=g, network_id='test1')
        driver.convert_networkx(network=g, network_id='test2')
        before_deletion = driver.query("MATCH (:Edge)-[r]-(:Network) RETURN count(r) as count")
        driver.delete_network(network_id='test2')
        after_deletion = driver.query("MATCH (:Edge)-[r]-(:Network) RETURN count(r) as count")
        driver.query("MATCH (n) DETACH DELETE n")
        self.assertGreater(before_deletion[0]['count'], after_deletion[0]['count'])

    def test_add_metadata_from_file(self):
        """
        Starts the Io driver
        and checks if the metadata is uploaded correctly.
        :return:
        """
        driver = Biom2Neo(user='neo4j',
                          password='test',
                          uri='bolt://localhost:7688', filepath=_resource_path(''),
                          encrypted=False)
        driver.convert_biom(testbiom, exp_id='test')
        inputs = {'networks': None,
                  'fp': _resource_path(''),
                  'username': 'neo4j',
                  'password': 'test',
                  'address': 'bolt://localhost:7688',
                  'delete': None,
                  'store_config': False,
                  'cyto': None,
                  'fasta': None,
                  'meta': ['test.tsv'],
                  'write': None,
                  'encryption': False}
        driver = IoDriver(user=inputs['username'],
                          password=inputs['password'],
                          uri=inputs['address'], filepath=inputs['fp'],
                          encrypted=False)
        driver.convert_networkx(network=g, network_id='test')
        start_io(inputs)
        test = driver.query("MATCH (:Taxon)-[r]-(:Property {name: 'Fruit'}) RETURN count(r) as count")
        driver.query("MATCH (n) DETACH DELETE n")
        self.assertEqual(test[0]['count'], 5)

    def test_add_metadata(self):
        """
        Starts the Io driver
        and checks if the metadata is uploaded correctly.
        :return:
        """
        driver = Biom2Neo(user='neo4j',
                          password='test',
                          uri='bolt://localhost:7688', filepath=_resource_path(''),
                          encrypted=False)
        driver.convert_biom(testbiom, exp_id='test')
        inputs = {'networks': ['test.graphml'],
                  'fp': _resource_path(''),
                  'username': 'neo4j',
                  'password': 'test',
                  'address': 'bolt://localhost:7688',
                  'delete': None,
                  'store_config': False,
                  'cyto': None,
                  'fasta': None,
                  'meta': None,
                  'write': None,
                  'encryption': False}
        driver = IoDriver(user=inputs['username'],
                          password=inputs['password'],
                          uri=inputs['address'], filepath=inputs['fp'],
                          encrypted=False)
        driver.convert_networkx(network=g, network_id='test')
        driver.include_nodes(nodes=testdict, name='Fruit', label='Taxon')
        test = driver.query("MATCH (:Taxon)-[r]-(:Property {name: 'Fruit'}) RETURN count(r) as count")
        driver.query("MATCH (n) DETACH DELETE n")
        self.assertEqual(test[0]['count'], 5)

    def test_export_network(self):
        """
        Starts the Io driver
        and checks if the network is exported as a dictionary.
        :return:
        """
        driver = Biom2Neo(user='neo4j',
                          password='test',
                          uri='bolt://localhost:7688', filepath=_resource_path(''),
                          encrypted=False)
        driver.convert_biom(testbiom, exp_id='test')
        inputs = {'networks': ['test.graphml'],
                  'fp': _resource_path(''),
                  'username': 'neo4j',
                  'password': 'test',
                  'address': 'bolt://localhost:7688',
                  'delete': None,
                  'store_config': False,
                  'cyto': None,
                  'fasta': None,
                  'meta': None,
                  'write': None,
                  'encryption': False}
        driver = IoDriver(user=inputs['username'],
                          password=inputs['password'],
                          uri=inputs['address'], filepath=inputs['fp'],
                          encrypted=False)
        driver.convert_networkx(network=g, network_id='test')
        result = driver.export_network(_resource_path(''), networks=['test'])
        self.assertTrue('test' in result)

    def test_write_network(self):
        """
        Checks if the network is written to disk.
        :return:
        """
        driver = Biom2Neo(user='neo4j',
                          password='test',
                          uri='bolt://localhost:7688', filepath=_resource_path(''),
                          encrypted=False)
        driver.convert_biom(testbiom, exp_id='test')
        inputs = {'networks': ['test'],
                  'fp': _resource_path(''),
                  'username': 'neo4j',
                  'password': 'test',
                  'address': 'bolt://localhost:7688',
                  'delete': None,
                  'store_config': False,
                  'cyto': None,
                  'fasta': None,
                  'meta': None,
                  'write': True,
                  'encryption': False}
        driver = IoDriver(user=inputs['username'],
                          password=inputs['password'],
                          uri=inputs['address'], filepath=inputs['fp'],
                          encrypted=False)
        driver.convert_networkx(network=g, network_id='test')
        driver.query("MATCH (n:Taxon {name: 'GG_OTU_1'})--(b:Edge) DETACH DELETE b")
        driver.query("MATCH (n:Taxon {name: 'GG_OTU_1'}) DETACH DELETE n")
        start_io(inputs)
        network = nx.read_graphml(_resource_path('test.graphml'))
        nx.write_graphml(g, _resource_path('test.graphml'))
        self.assertFalse('GG_OTU_1' in network.nodes)

    def test_import_network(self):
        """
        Checks if the network is written to the Neo4j database.
        :return:
        """
        driver = Biom2Neo(user='neo4j',
                          password='test',
                          uri='bolt://localhost:7688', filepath=_resource_path(''),
                          encrypted=False)
        driver.convert_biom(testbiom, exp_id='test')
        inputs = {'networks': ['test'],
                  'fp': _resource_path(''),
                  'username': 'neo4j',
                  'password': 'test',
                  'address': 'bolt://localhost:7688',
                  'delete': None,
                  'store_config': False,
                  'cyto': None,
                  'fasta': None,
                  'meta': None,
                  'write': True,
                  'encryption': False}
        driver = IoDriver(user=inputs['username'],
                          password=inputs['password'],
                          uri=inputs['address'], filepath=inputs['fp'],
                          encrypted=False)
        driver.convert_networkx(network=g, network_id='test')
        test = driver.query("MATCH (n:Network) RETURN n")
        driver.query("MATCH (n) DETACH DELETE n")
        self.assertEqual(len(test), 1)


if __name__ == '__main__':
    unittest.main()


