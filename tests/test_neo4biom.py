"""
This file contains functions for testing functions in the neo4biom.py script.

The file first sets up a simple Neo4j database for carrying out the tests.

"""

import unittest
import time
import os
import biom
import pandas as pd
import numpy as np
from biom.cli.util import write_biom_table
from mako.scripts.neo4biom import start_biom, Biom2Neo, read_taxonomy
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
--name=neo4j_test \
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


class TestNeo4Biom(unittest.TestCase):
    """
    Tests Base methods.
    Warning: most of these functions are to start a local database.
    Therefore, the presence of the necessary local files is a prerequisite.
    """
    @classmethod
    def setUpClass(cls):
        os.system(docker_command)
        write_biom_table(testbiom, filepath=_resource_path('test.hdf5'), fmt='hdf5')
        obs_data = testbiom.matrix_data
        data = [pd.Series(obs_data[i].toarray().ravel()) for i in np.arange(obs_data.shape[0])]
        obs_data = pd.DataFrame(data, index=testbiom.ids(axis='observation'))
        obs_data.columns = testbiom.ids(axis='sample')
        obs_data.to_csv(_resource_path('test.tsv'), sep='\t')
        with open(_resource_path('test.tsv'), 'r') as original:
            data = original.read()
        with open(_resource_path('test.tsv'), 'w') as modified:
            modified.write("#" + data)
        taxdata = testbiom.metadata_to_dataframe(axis='observation')
        taxdata.to_csv(_resource_path('test_tax.tsv'), sep='\t')
        time.sleep(20)

    @classmethod
    def tearDownClass(cls):
        os.system('docker stop neo4j_test')
        os.remove(_resource_path('test.hdf5'))
        os.remove(_resource_path('test.tsv'))
        os.remove(_resource_path('test_tax.tsv'))

    def test_start_biom(self):
        """
        Checks if the BIOM file is correctly uploaded to the database.
        :return:
        """
        inputs = {'biom_file': [_resource_path('test.hdf5')],
                  'fp': _resource_path(''),
                  'count_table': None,
                  'tax_table': None,
                  'sample_meta': None,
                  'taxon_meta': None,
                  'qza': None,
                  'username': 'neo4j',
                  'password': 'test',
                  'address': 'bolt://localhost:7688',
                  'store_config': False,
                  'delete': None,
                  'encryption': False,
                  'obs': True}
        start_biom(inputs)
        driver = Biom2Neo(user=inputs['username'],
                          password=inputs['password'],
                          uri=inputs['address'], filepath=inputs['fp'],
                          encrypted=False)
        test = driver.query("MATCH (n:Experiment) RETURN n")
        driver.write("MATCH (n) DETACH DELETE n")
        self.assertEqual(test[0]['n']['name'], 'test')

    def test_start_biom_tabs(self):
        """
        Checks if the BIOM file, if imported as tab-delimited files,
        is correctly uploaded to the database.
        :return:
        """
        inputs = {'biom_file': None,
                  'fp': _resource_path(''),
                  'count_table': [_resource_path('test.tsv')],
                  'tax_table': None,
                  'sample_meta': None,
                  'taxon_meta': None,
                  'qza': None,
                  'username': 'neo4j',
                  'password': 'test',
                  'address': 'bolt://localhost:7688',
                  'store_config': False,
                  'delete': None,
                  'encryption': False,
                  'obs': True}
        start_biom(inputs)
        driver = Biom2Neo(user=inputs['username'],
                          password=inputs['password'],
                          uri=inputs['address'], filepath=inputs['fp'],
                          encrypted=False)
        test = driver.query("MATCH (n:Experiment) RETURN n")
        driver.write("MATCH (n) DETACH DELETE n")
        self.assertEqual(test[0]['n']['name'], 'test')

    def test_start_taxonomy(self):
        """
        Checks if the taxonomy file is correctly uploaded to the database.
        :return:
        """
        inputs = {'biom_file': None,
                  'fp': _resource_path(''),
                  'count_table': None,
                  'tax_table': [_resource_path('test_tax.tsv')],
                  'sample_meta': None,
                  'taxon_meta': None,
                  'qza': None,
                  'username': 'neo4j',
                  'password': 'test',
                  'address': 'bolt://localhost:7688',
                  'store_config': False,
                  'delete': None,
                  'encryption': False,
                  'obs': True}
        start_biom(inputs)
        driver = Biom2Neo(user=inputs['username'],
                          password=inputs['password'],
                          uri=inputs['address'], filepath=inputs['fp'],
                          encrypted=False)
        test = driver.query("MATCH (n:Experiment) RETURN n")
        driver.write("MATCH (n) DETACH DELETE n")
        self.assertEqual(test[0]['n']['name'], 'test_tax')

    def test_taxonomy(self):
        """
        Checks if the taxonomy file is correctly uploaded to the database.
        :return:
        """
        inputs = {'biom_file': None,
                  'fp': _resource_path(''),
                  'count_table': None,
                  'tax_table': [_resource_path('test_tax.tsv')],
                  'sample_meta': None,
                  'taxon_meta': None,
                  'qza': None,
                  'username': 'neo4j',
                  'password': 'test',
                  'address': 'bolt://localhost:7688',
                  'store_config': False,
                  'delete': None,
                  'encryption': False}
        driver = Biom2Neo(user=inputs['username'],
                          password=inputs['password'],
                          uri=inputs['address'], filepath=inputs['fp'],
                          encrypted=False)
        name, taxtab = read_taxonomy(inputs['tax_table'][0], inputs['fp'])
        driver.convert_taxonomy(taxtab, name)
        test = driver.query("MATCH (n:Order {name: 'o__Enterobacteriales'})"
                            "--(:Taxon) RETURN n")
        driver.write("MATCH (n) DETACH DELETE n")
        self.assertEqual(test[0]['n']['name'], 'o__Enterobacteriales')

    def test_delete_correct_biom(self):
        """
        Checks if only the correct BIOM file is deleted.
        :return:
        """
        inputs = {'biom_file': None,
                  'fp': _resource_path(''),
                  'count_table': None,
                  'tax_table': None,
                  'sample_meta': None,
                  'taxon_meta': None,
                  'qza': None,
                  'username': 'neo4j',
                  'password': 'test',
                  'address': 'bolt://localhost:7688',
                  'store_config': False,
                  'delete': ['test1'],
                  'encryption': False,
                  'obs': True}
        driver = Biom2Neo(user=inputs['username'],
                          password=inputs['password'],
                          uri=inputs['address'], filepath=inputs['fp'],
                          encrypted=False)
        driver.convert_biom(testbiom, 'test1')
        driver.convert_biom(testbiom, 'test2')
        start_biom(inputs)
        test = driver.query("MATCH (n:Experiment {name: 'test2'}) RETURN n")
        driver.write("MATCH (n) DETACH DELETE n")
        self.assertEqual(len(test), 1)

    def test_convert_biom(self):
        """
        Starts the Biom driver
        and checks if the correct number of samples is in the database.
        :return:
        """
        inputs = {'biom_file': None,
                  'fp': _resource_path(''),
                  'count_table': None,
                  'tax_table': None,
                  'sample_meta': None,
                  'taxon_meta': None,
                  'qza': None,
                  'username': 'neo4j',
                  'password': 'test',
                  'address': 'bolt://localhost:7688',
                  'store_config': False,
                  'delete': ['test1'],
                  'encryption': False}
        driver = Biom2Neo(user=inputs['username'],
                          password=inputs['password'],
                          uri=inputs['address'], filepath=inputs['fp'],
                          encrypted=False)
        driver.convert_biom(testbiom, 'test')
        test = driver.query("MATCH (n:Specimen) RETURN count(n) as count")
        driver.write("MATCH (n) DETACH DELETE n")
        self.assertEqual(test[0]['count'], 6)

    def test_delete_biom(self):
        """
        Starts the Biom driver
        and checks if the Experiment is deleted.
        :return:
        """
        inputs = {'biom_file': None,
                  'fp': _resource_path(''),
                  'count_table': None,
                  'tax_table': None,
                  'sample_meta': None,
                  'taxon_meta': None,
                  'qza': None,
                  'username': 'neo4j',
                  'password': 'test',
                  'address': 'bolt://localhost:7688',
                  'store_config': False,
                  'delete': ['test1'],
                  'encryption': False}
        driver = Biom2Neo(user=inputs['username'],
                          password=inputs['password'],
                          uri=inputs['address'], filepath=inputs['fp'],
                          encrypted=False)
        driver.convert_biom(testbiom, 'test1')
        driver.convert_biom(testbiom, 'test2')
        driver.delete_biom(exp_id='test1')
        test = driver.query("MATCH (n:Experiment {name: 'test1'}) RETURN n")
        driver.write("MATCH (n) DETACH DELETE n")
        self.assertEqual(len(test), 0)

    def test_biom_property(self):
        """
        Uploads the BIOM data and checks if the metadata is also added.
        :return:
        """
        inputs = {'biom_file': None,
                  'fp': _resource_path(''),
                  'count_table': None,
                  'tax_table': None,
                  'sample_meta': None,
                  'taxon_meta': None,
                  'qza': None,
                  'username': 'neo4j',
                  'password': 'test',
                  'address': 'bolt://localhost:7688',
                  'store_config': False,
                  'delete': ['test1'],
                  'encryption': False}
        driver = Biom2Neo(user=inputs['username'],
                          password=inputs['password'],
                          uri=inputs['address'], filepath=inputs['fp'],
                          encrypted=False)
        driver.convert_biom(testbiom, 'test')
        test = driver.query("MATCH (n:Property {name: 'Description'})-[r]-(:Specimen) RETURN count(r) as count")
        driver.query("MATCH (n:) DETACH DELETE n")
        self.assertEqual(test[0]['count'], 6)


if __name__ == '__main__':
    unittest.main()


