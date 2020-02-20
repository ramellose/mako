"""
This file contains functions for testing functions in the neo4biom.py script.

The file first sets up a simple Neo4j database for carrying out the tests.

"""

import unittest
import os
import biom
from biom.cli.util import write_biom_table
from mako.scripts.neo4biom import start_biom, Biom2Neo
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


tabotu = '[[ 243  567  112   45   2]\n ' \
         '[ 235   56  788  232    1]\n ' \
         '[4545   22    0    1    0]\n ' \
         '[  41   20    2    4    0]]'

tabtax = "[['k__Bacteria' 'p__Firmicutes' 'c__Clostridia' 'o__Clostridiales'\n  " \
         "'f__Clostridiaceae' 'g__Anaerococcus' 's__']\n " \
         "['k__Bacteria' 'p__Bacteroidetes' 'c__Bacteroidia' 'o__Bacteroidales'\n  " \
         "'f__Prevotellaceae' 'g__Prevotella' 's__']\n " \
         "['k__Bacteria' 'p__Proteobacteria' 'c__Alphaproteobacteria'\n  " \
         "'o__Sphingomonadales' 'f__Sphingomonadaceae' 'g__Sphingomonas' 's__']\n " \
         "['k__Bacteria' 'p__Verrucomicrobia' 'c__Verrucomicrobiae'\n  " \
         "'o__Verrucomicrobiales' 'f__Verrucomicrobiaceae' 'g__Luteolibacter' 's__']]"

tabmeta = "[['Australia' 'Hot']\n " \
          "['Antarctica' 'Cold']\n " \
          "['Netherlands' 'Rainy']\n " \
          "['Belgium' 'Rainy']\n " \
          "['Iceland' 'Cold']]"

sample_ids = ['S%d' % i for i in range(1, 6)]
observ_ids = ['O%d' % i for i in range(1, 5)]

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
                  'username': 'neo4j',
                  'password': 'test',
                  'address': 'bolt://localhost:7688',
                  'store_config': False,
                  'delete': None}
        start_biom(inputs)
        driver = Biom2Neo(user=inputs['username'],
                          password=inputs['password'],
                          uri=inputs['address'], filepath=inputs['fp'],
                          encrypted=False)
        driver.query("MATCH (n:Experiment) RETURN n")
        outcome = driver.check_domain_range()
        start_base(inputs)
        self.assertTrue(outcome)

    def test_start_biom_tabs(self):
        """
        Checks if the BIOM file, if imported as tab-delimited files,
        is correctly uploaded to the database.
        :return:
        """
        inputs = {'fp': loc + '/Documents/mako_files',
                  'neo4j': loc + 'Documents//neo4j',
                  'username': 'neo4j',
                  'password': 'test',
                  'address': 'bolt://localhost:7688',
                  'start': False,
                  'clear': True,
                  'quit': False,
                  'store_config': True,
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

    def test_delete_biom(self):
        """
        Checks if only the correct BIOM file is deleted.
        :return:
        """

    def test_convert_biom(self):
        """
        Starts the Biom driver
        and checks if the correct number of samples is in the database.
        :return:
        """

    def test_delete_biom(self):
        """
        Starts the Biom driver
        and checks if the Experiment is deleted.
        :return:
        """

    def test_biom_property(self):
        """
        Uploads the BIOM data and checks if the metadata is also added.
        :return:
        """


if __name__ == '__main__':
    os.system(docker_command)
    write_biom_table(testbiom, filepath=_resource_path('test.hdf5'), fmt='hdf5')
    data = testbiom.to_dataframe()
    data.to_csv(_resource_path('test.tsv'), sep='\t')
    testbiom.to_tsv(_resource_path('test.tsv'))
    unittest.main()
    os.system('docker stop neo4j')
    os.remove(_resource_path('test.hdf5'))
    os.remove(_resource_path('test.tsv'))
    os.remove(_resource_path('mako.log'))

