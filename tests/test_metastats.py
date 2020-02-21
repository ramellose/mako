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
from mako.scripts.metastats import start_metastats, MetastatsDriver
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
        driver.convert_networkx(network=f, network_id='f')

    @classmethod
    def tearDownClass(cls):
        driver = Biom2Neo(user='neo4j',
                          password='test',
                          uri='bolt://localhost:7688', filepath=_resource_path(''),
                          encrypted=False)
        driver.query('MATCH (n) DETACH DELETE n')
        os.system('docker stop neo4j')

    def test_start_metastats(self):
        """
        Checks if the number of taxon nodes in the Family network is smaller
        after running metastats.
        :return:
        """
        inputs = {'networks': None,
                  'fp': _resource_path(''),
                  'username': 'neo4j',
                  'password': 'test',
                  'address': 'bolt://localhost:7688',
                  'store_config': False,
                  'variable': None,
                  'weight': True,
                  'agglom': 'family',
                  'encryption': False}
        start_metastats(inputs)
        driver = IoDriver(user='neo4j',
                          password='test',
                          uri='bolt://localhost:7688', filepath=_resource_path(''),
                          encrypted=False)
        test = driver.query("MATCH (n:Network)-[:AGGLOMERATED]->() RETURN n")
        for id in test:
            driver.delete_network(network_id=id['n']['name'])
        self.assertEqual(len(test), 4)

    def test_agglomerate_weight(self):
        """
        Checks if the correct number of edges is in the agglomerated network.
        There should be one selfloop (Escherichia-Escherichia)
        with the above toy network.
        is deleted.
        :return:
        """
        driver = MetastatsDriver(user='neo4j',
                                 password='test',
                                 uri='bolt://localhost:7688', filepath=_resource_path(''),
                                 encrypted=False)
        driver.agglomerate_networks(networks=['g'], weight=True, level='Genus')
        driver = IoDriver(user='neo4j',
                          password='test',
                          uri='bolt://localhost:7688', filepath=_resource_path(''),
                          encrypted=False)
        genus = driver.query("MATCH (n:Network {name: 'Genus_g'})--()--(b:Taxon) RETURN b")
        genus = [x['b']['name'] for x in genus]
        orig = driver.query("MATCH (n:Network {name: 'g'})--()--(b:Taxon) RETURN b")
        orig = [x['b']['name'] for x in orig]
        driver.delete_network(network_id='Genus_g')
        self.assertGreater(len(set(orig)), len(set(genus)))

    def test_agglomerate_no_weight(self):
        """
        Checks if the correct number of edges is in the agglomerated network.
        There should be one selfloop (Escherichia-Escherichia)
        with the above toy network.
        Additionally, the edge between the Escherichia node and the two
        f__Punk nodes should be merged.
        :return:
        """
        driver = MetastatsDriver(user='neo4j',
                                 password='test',
                                 uri='bolt://localhost:7688', filepath=_resource_path(''),
                                 encrypted=False)
        tax_list = ['Species', 'Genus', 'Family', 'Order', 'Class', 'Phylum', 'Kingdom']
        networks = ['g']
        for level in range(0, 2 + 1):
            # pub.sendMessage('update', msg="Agglomerating edges...")
            networks = driver.agglomerate_networks(level=tax_list[level], weight=False, networks=networks)
            # networks assignment contains names of new networks
        driver = IoDriver(user='neo4j',
                          password='test',
                          uri='bolt://localhost:7688', filepath=_resource_path(''),
                          encrypted=False)
        genus = driver.query("MATCH (n:Network {name: 'Genus_g'})--(a)--(b:Taxon) RETURN a")
        genus = [x['a']['name'] for x in genus]
        family = driver.query("MATCH (n:Network {name: 'Family_g'})--(a)--(b:Taxon) RETURN a")
        family = [x['a']['name'] for x in family]
        driver.delete_network(network_id='Genus_g')
        driver.delete_network(network_id='Family_g')
        self.assertGreater(len(set(genus)), len(set(family)))

    def test_agglomerate_phylum(self):
        """
        There are no edges to merge beyond Family level,
        so those networks should not be created.
        :return:
        """
        driver = MetastatsDriver(user='neo4j',
                                 password='test',
                                 uri='bolt://localhost:7688', filepath=_resource_path(''),
                                 encrypted=False)
        tax_list = ['Species', 'Genus', 'Family', 'Order', 'Class', 'Phylum', 'Kingdom']
        networks = ['g']
        for level in range(0, 5 + 1):
            # pub.sendMessage('update', msg="Agglomerating edges...")
            networks = driver.agglomerate_networks(level=tax_list[level], weight=True, networks=networks)
            # networks assignment contains names of new networks
        driver = IoDriver(user='neo4j',
                          password='test',
                          uri='bolt://localhost:7688', filepath=_resource_path(''),
                          encrypted=False)
        test = driver.query("MATCH (n:Network {name: 'Class_g'})--(a)--(b:Taxon) RETURN a")
        driver.delete_network(network_id='Genus_g')
        driver.delete_network(network_id='Family_g')
        self.assertEqual(len(test), 0)

    def test_qual_variable(self):
        """
        Checks if the hypergeometic test links are added.
        :return:
        """
        driver = MetastatsDriver(user='neo4j',
                                 password='test',
                                 uri='bolt://localhost:7688', filepath=_resource_path(''),
                                 encrypted=False)
        variables = set([x[y] for x in driver.query("MATCH (n:Property) RETURN n.type") for y in x])
        for var in variables:
            driver.associate_samples(label=var)
        test = driver.query("MATCH (n:Taxon)-[r:HYPERGEOM]-(:Property) RETURN count(r) as count")
        driver.query("MATCH (n:Taxon)-[r]-(b:Property) DETACH DELETE b")
        self.assertEqual(test[0]['count'], 3)

    def test_quant_variable(self):
        """
        Checks if the hypergeometic test links are added.
        :return:
        """
        driver = MetastatsDriver(user='neo4j',
                                 password='test',
                                 uri='bolt://localhost:7688', filepath=_resource_path(''),
                                 encrypted=False)
        variables = set([x[y] for x in driver.query("MATCH (n:Property) RETURN n.type") for y in x])
        for var in variables:
            driver.associate_samples(label=var)
        test = driver.query("MATCH (n:Taxon)-[r:SPEARMAN]-(:Property) RETURN count(r) as count")
        driver.query("MATCH (n:Taxon)-[r]-(b:Property) DETACH DELETE b")
        self.assertEqual(test[0]['count'], 1)


if __name__ == '__main__':
    unittest.main()


