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
from mako.scripts.wrapper import start_wrapper, run_manta, run_anuran
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
     "id":null,
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
        {"id":"GG_OTU_3", "metadata":{"taxonomy":["k__Bacteria", "p__Firmicute\
s", "c__Clostridia", "o__Halanaerobiales", "f__Punk", "\
g_Anthrax", "s__"]}},
        {"id":"GG_OTU_4", "metadata":{"taxonomy":["k__Bacteria", "p__Firmicute\
s", "c__Clostridia", "o__Halanaerobiales", "f__Punk", "g__NOFX", "s__"]}},
        {"id":"GG_OTU_5", "metadata":{"taxonomy":["k__Bacteria", "p__Proteobac\
teria", "c__Gammaproteobacteria", "o__Enterobacteriales", "f__Enterobacteriace\
ae", "g__Escherichia", "s__"]}}
        ],
     "columns":[
        {"id":"Sample1", "metadata":{
                                "pH":"2.0",
                                "BarcodeSequence":"CGCTTATCGAGA",
                                "LinkerPrimerSequence":"CATGCTGCCTCCCGTAGGAGT",
                                "BODY_SITE":"gut",
                                "Description":"human gut"}},
        {"id":"Sample2", "metadata":{
                                "pH":"1.8",       
                                "BarcodeSequence":"CATACCAGTAGC",
                                "LinkerPrimerSequence":"CATGCTGCCTCCCGTAGGAGT",
                                "BODY_SITE":"gut",
                                "Description":"human gut"}},
        {"id":"Sample3", "metadata":{
                                "pH":"2.3",        
                                "BarcodeSequence":"CTCTCTACCTGT",
                                "LinkerPrimerSequence":"CATGCTGCCTCCCGTAGGAGT",
                                "BODY_SITE":"gut",
                                "Description":"human gut"}},
        {"id":"Sample4", "metadata":{
                                "pH":"2.1",        
                                "BarcodeSequence":"CGCTTATCGAGA",
                                "LinkerPrimerSequence":"CATGCTGCCTCCCGTAGGAGT",
                                "BODY_SITE":"gut",
                                "Description":"human gut"}},
        {"id":"Sample5", "metadata":{
                                "pH":"2.0",        
                                "BarcodeSequence":"CATACCAGTAGC",
                                "LinkerPrimerSequence":"CATGCTGCCTCCCGTAGGAGT",
                                "BODY_SITE":"gut",
                                "Description":"human gut"}},
        {"id":"Sample6", "metadata":{
                                "pH":"2.1",        
                                "BarcodeSequence":"CTCTCTACCTGT",
                                "LinkerPrimerSequence":"CATGCTGCCTCCCGTAGGAGT",
                                "BODY_SITE":"gut",
                                "Description":"human gut"}},
        {"id":"Sample7", "metadata":{
                                "pH":"1.9",        
                                "BarcodeSequence":"CTCTCTACCTGT",
                                "LinkerPrimerSequence":"CATGCTGCCTCCCGTAGGAGT",
                                "BODY_SITE":"gut",
                                "Description":"human gut"}},
        {"id":"Sample8", "metadata":{
                                "pH":"1.9",        
                                "BarcodeSequence":"CTCTCTACCTGT",
                                "LinkerPrimerSequence":"CATGCTGCCTCCCGTAGGAGT",
                                "BODY_SITE":"gut",
                                "Description":"human gut"}},    
        {"id":"Sample9", "metadata":{
                                "pH":"1.8",        
                                "BarcodeSequence":"CTCTCTACCTGT",
                                "LinkerPrimerSequence":"CATGCTGCCTCCCGTAGGAGT",
                                "BODY_SITE":"gut",
                                "Description":"human gut"}},
        {"id":"Sample10", "metadata":{
                                "pH":"2.1",        
                                "BarcodeSequence":"CTCTCTACCTGT",
                                "LinkerPrimerSequence":"CATGCTGCCTCCCGTAGGAGT",
                                "BODY_SITE":"gut",
                                "Description":"human gut"}},                                    
        {"id":"Sample11", "metadata":{
                                "pH":"6.8",        
                                "BarcodeSequence":"CTCTCTACCAAT",
                                "LinkerPrimerSequence":"CATGCTGCCTCCCGTAGGAGT",
                                "BODY_SITE":"skin",
                                "Description":"human skin"}},
        {"id":"Sample12", "metadata":{
                                "pH":"6.9",        
                                "BarcodeSequence":"CTAACTACCAAT",
                                "LinkerPrimerSequence":"CATGCTGCCTCCCGTAGGAGT",
                                "BODY_SITE":"skin",
                                "Description":"human skin"}},
        {"id":"Sample13", "metadata":{
                                "pH":"7.1",        
                                "BarcodeSequence":"CTCTCGGCCTGT",
                                "LinkerPrimerSequence":"CATGCTGCCTCCCGTAGGAGT",
                                "BODY_SITE":"skin",
                                "Description":"human skin"}},
        {"id":"Sample14", "metadata":{
                                "pH":"7.0",        
                                "BarcodeSequence":"CTCTCTACCAAT",
                                "LinkerPrimerSequence":"CATGCTGCCTCCCGTAGGAGT",
                                "BODY_SITE":"skin",
                                "Description":"human skin"}},
        {"id":"Sample15", "metadata":{
                                "pH":"6.8",        
                                "BarcodeSequence":"CTCTCTACCAAT",
                                "LinkerPrimerSequence":"CATGCTGCCTCCCGTAGGAGT",
                                "BODY_SITE":"skin",
                                "Description":"human skin"}},
        {"id":"Sample16", "metadata":{
                                "pH":"6.9",        
                                "BarcodeSequence":"CTCTCTACCAAT",
                                "LinkerPrimerSequence":"CATGCTGCCTCCCGTAGGAGT",
                                "BODY_SITE":"skin",
                                "Description":"human skin"}},
        {"id":"Sample17", "metadata":{
                                "pH":"6.7",        
                                "BarcodeSequence":"CTCTCTACCAAT",
                                "LinkerPrimerSequence":"CATGCTGCCTCCCGTAGGAGT",
                                "BODY_SITE":"skin",
                                "Description":"human skin"}},   
        {"id":"Sample18", "metadata":{
                                "pH":"7.2",        
                                "BarcodeSequence":"CTCTCTACCAAT",
                                "LinkerPrimerSequence":"CATGCTGCCTCCCGTAGGAGT",
                                "BODY_SITE":"skin",
                                "Description":"human skin"}},
        {"id":"Sample19", "metadata":{
                                "pH":"6.8",        
                                "BarcodeSequence":"CTCTCTACCAAT",
                                "LinkerPrimerSequence":"CATGCTGCCTCCCGTAGGAGT",
                                "BODY_SITE":"skin",
                                "Description":"human skin"}},                                                                                                                         
        {"id":"Sample20", "metadata":{
                                "pH":"7.0",        
                                "BarcodeSequence":"CTAACTACCAAT",
                                "LinkerPrimerSequence":"CATGCTGCCTCCCGTAGGAGT",
                                "BODY_SITE":"skin",
                                "Description":"human skin"}}
        ],
     "matrix_type": "sparse",
     "matrix_element_type": "int",
     "shape": [5, 20],
     "data":[[0,10,5],
             [0,11,5],
             [0,12,6],
             [0,13,5],
             [0,14,5],
             [0,15,5],
             [0,16,6],
             [0,17,5],
             [0,18,5],
             [0,19,6],
             [0,9,6],
             [1,0,5],
             [1,1,1],
             [1,3,2],
             [1,4,3],
             [1,8,5],
             [1,10,1],
             [1,11,2],
             [1,2,3],
             [1,14,5],
             [1,17,1],
             [1,12,2],
             [1,19,1],
             [2,2,1],
             [2,3,4],
             [2,5,2],
             [2,6,1],
             [2,8,4],
             [2,10,2],
             [2,14,4],
             [2,16,2],
             [3,0,2],
             [3,1,1],
             [3,2,1],
             [3,5,1],
             [3,7,2],
             [3,12,1],
             [3,15,2],
             [3,7,1],
             [3,10,1],
             [3,11,1],
             [4,1,1],
             [4,2,1],
             [4,4,1],
             [4,14,1],
             [4,6,1]
            ]
    }
"""

testbiom = biom.parse.parse_biom_table(testraw)

# make toy network
g = nx.Graph()
nodes = ["GG_OTU_1", "GG_OTU_2", "GG_OTU_3", "GG_OTU_4", "GG_OTU_5"]
g.add_nodes_from(nodes)
g.add_edges_from([("GG_OTU_1", "GG_OTU_2"),
                  ("GG_OTU_2", "GG_OTU_3"), ("GG_OTU_4", "GG_OTU_1")])
g["GG_OTU_1"]["GG_OTU_2"]['weight'] = 1.0
g["GG_OTU_2"]["GG_OTU_3"]['weight'] = 1.0
g["GG_OTU_4"]["GG_OTU_1"]['weight'] = -1.0

f = g.copy(as_view=False)
g["GG_OTU_1"]["GG_OTU_2"]['weight'] = -1.0


class TestWrapper(unittest.TestCase):
    """
    Tests wrapper methods.
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
        driver.convert_networkx(network=f, network_id='f')
        driver.convert_networkx(network=g, network_id='h')
        driver.convert_networkx(network=g, network_id='i')
        driver.convert_networkx(network=f, network_id='j')


    @classmethod
    def tearDownClass(cls):
        driver = Biom2Neo(user='neo4j',
                          password='test',
                          uri='bolt://localhost:7688', filepath=_resource_path(''),
                          encrypted=False)
        driver.query('MATCH (n) DETACH DELETE n')
        os.system('docker stop neo4j')

    def test_start_wrapper(self):
        """
        Checks if the cluster and centrality nodes have been added
        after running the wrapper.
        :return:
        """
        inputs = {'networks': None,
                  'fp': _resource_path(''),
                  'username': 'neo4j',
                  'password': 'test',
                  'address': 'bolt://localhost:7688',
                  'store_config': True,
                  'min': 2,
                  'ms': 0.2,
                  'max': 2,
                  'limit': 2,
                  'iter': 20,
                  'perm': None,
                  'subset': 0.8,
                  'ratio': 0.8,
                  'edgescale': 0.8,
                  'cr': False,
                  'rel': 20,
                  'error': 0.1,
                  'b': False,
                  'manta': True,
                  'anuran': False,
                  'encryption': False
                  }
        start_wrapper(inputs)
        driver = IoDriver(user='neo4j',
                          password='test',
                          uri='bolt://localhost:7688', filepath=_resource_path(''),
                          encrypted=False)
        test = driver.query("MATCH (n:Property {type: 'cluster'}) RETURN n")
        for id in test:
            driver.query(("MATCH "))
        self.assertEqual(len(test), 2)



if __name__ == '__main__':
    unittest.main()


