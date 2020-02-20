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
import argparse
import multiprocessing as mp
from mako.scripts.base import start_base
from mako.scripts.neo4biom import start_biom
from mako.scripts.io import start_io
#from mako.scripts.netstats import start_netstats
#from mako.scripts.metastats import start_metastats
import logging.handlers

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# handler to sys.stdout
sh = logging.StreamHandler(sys.stdout)
sh.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
sh.setFormatter(formatter)
logger.addHandler(sh)


def mako(mako_args):
    """
    Main function for running mako.
    Accepts a dictionary of arguments from the argument parser
    and calls the appropriate module function.

    :param mako_args: Arguments.
    :return:
    """
    # handler to file
    # construct logger after filepath is provided
    if 'base' in mako_args:
        logger.info('Running base Neo4j module. ')
        start_base(mako_args)
    if 'neo4biom' in mako_args:
        logger.info('Running Neo4biom module. ')
        start_biom(mako_args)
    if 'io' in mako_args:
        logger.info('Running IO module. ')
        start_io(mako_args)
    #if 'netstats' in mako_args:
    #    logger.info('Performing network analysis on Neo4j database. ')
    #    run_netstats(mako_args)
    #if 'metastats' in mako_args:
    #    logger.info('Performing metadata analysis on Neo4j database. ')
    #    run_metastats(mako_args)
    logger.info('Completed tasks! ')


mako_parser = argparse.ArgumentParser(description='mako pipeline')
mako_parser.add_argument('-s', '--set',
                         dest='settings',
                         help='Settings txt file containing '
                              'filepaths to processed data, '
                              'as well as other settings. ',
                         default=os.getcwd() + '\\config')
subparsers = mako_parser.add_subparsers(title="mako modules",
                                        description="Each module carries out a part of mako. "
                                                    "Modules can be used independently "
                                                    "as long as the correct settings file "
                                                    "is provided and the Neo4j instance is running. ")

parse_base = subparsers.add_parser('base', description='Start, clear and quit the Neo4j database.',
                                   help='The base module runs the Neo4j console and carries out '
                                        'checks to validate the database consistency with the schema. '
                                        'These checks are especially valuable when you are manually '
                                        'editing the database.')
parse_base.add_argument('-fp', '--output_filepath',
                        dest='fp',
                        help='File path for importing and / or exporting files. ',
                        default=None)
parse_base.add_argument('-cf', '--config',
                        dest='config',
                        action='store_true',
                        help='If true, store config files to reload Neo4j settings. ',
                        required=False,
                        default=None)
parse_base.add_argument('-n', '--neo4j',
                        dest='neo4j',
                        help='Filepath to neo4j folder. ',
                        required=False,
                        type=str,
                        default=None)
parse_base.add_argument('-u', '--username',
                        dest='username',
                        required=False,
                        help='Username for neo4j database access. ',
                        type=str,
                        default='neo4j')
parse_base.add_argument('-p', '--password',
                        dest='password',
                        required=False,
                        type=str,
                        help='Password for neo4j database access. ')
parse_base.add_argument('-a', '--address',
                        dest='address',
                        required=False,
                        help='Address for neo4j database. ',
                        type=str,
                        default='bolt://localhost:7687')
parse_base.add_argument('-start', '--start',
                        dest='start',
                        action='store_true',
                        help='If true, start Neo4j database.',
                        required=False,
                        default=None)
parse_base.add_argument('-clear', '--clear',
                        dest='clear',
                        action='store_true',
                        help='If true, clear Neo4j database.',
                        required=False,
                        default=None)
parse_base.add_argument('-quit', '--quit',
                        dest='quit',
                        action='store_true',
                        help='If true, shut down Neo4j database.',
                        required=False,
                        default=None)
parse_base.add_argument('-check', '--check_schema',
                        dest='check',
                        action='store_true',
                        help='If true, checks whether Neo4j database violates the schema.',
                        required=False,
                        default=None)


parse_neo4biom = subparsers.add_parser('biom', description='Read/write operations for standard '
                                                           'microbial abundance data formats.',
                                       help='The neo4biom module contains functions that read BIOM files '
                                       'or tab-delimited files and write these to the Neo4j database. '
                                       'It can also delete files by referencing their file name. ')
parse_neo4biom.add_argument('-fp', '--output_filepath',
                            dest='fp',
                            help='File path for importing and / or exporting files. ',
                            default=None)
parse_neo4biom.add_argument('-cf', '--config',
                            dest='config',
                            action='store_true',
                            help='If true, store config files to reload Neo4j settings. ',
                            required=False,
                            default=None)
parse_neo4biom.add_argument('-u', '--username',
                            dest='username',
                            required=False,
                            help='Username for neo4j database access. ',
                            type=str,
                            default='neo4j')
parse_neo4biom.add_argument('-p', '--password',
                            dest='password',
                            required=False,
                            type=str,
                            help='Password for neo4j database access. ')
parse_neo4biom.add_argument('-a', '--address',
                            dest='address',
                            required=False,
                            help='Address for neo4j database. ',
                            type=str,
                            default='bolt://localhost:7687')
parse_neo4biom.add_argument('-biom', '--biom_file',
                            dest='biom',
                            required=False,
                            help='One or more BIOM files. ',
                            type=list,
                            default=None)
parse_neo4biom.add_argument('-count', '--count_table',
                            dest='count',
                            required=False,
                            help='One or more tab-delimited count tables. ',
                            type=list,
                            default=None)
parse_neo4biom.add_argument('-tax', '--tax_table',
                            dest='tax',
                            required=False,
                            help='One or more tab-delimited taxonomy tables. ',
                            type=list,
                            default=None)
parse_neo4biom.add_argument('-tm', '--taxon_metadata',
                            dest='taxon_meta',
                            required=False,
                            help='One or more tab-delimited taxon metadata tables. ',
                            type=list,
                            default=None)
parse_neo4biom.add_argument('-sm', '--sample_metadata',
                            dest='sample_meta',
                            required=False,
                            help='One or more tab-delimited sample metadata tables. ',
                            type=list,
                            default=None)
parse_neo4biom.add_argument('-del', '--delete',
                            dest='delete',
                            required=False,
                            help='Names of count tables (without full path) to delete from the database. ',
                            type=list,
                            default=None)


parse_io = subparsers.add_parser('io', description='Read/write operations to disk, Cytoscape and Neo4j.',
                                 help='The io module contains functions that change the type of'
                                      ' data storage; for example, reading a network from an edge list to '
                                      'the Neo4j database, or exporting a Neo4j network to Cytoscape. '
                                      'To export to Cytoscape, you need to have started the software first. ')
parse_io.add_argument('-fp', '--output_filepath',
                      dest='fp',
                      help='File path for importing and / or exporting files. ',
                      default=None)
parse_io.add_argument('-cf', '--config',
                      dest='config',
                      action='store_true',
                      help='If true, store config files to reload Neo4j settings. ',
                      required=False,
                      default=None)
parse_io.add_argument('-u', '--username',
                      dest='username',
                      required=False,
                      help='Username for neo4j database access. ',
                      type=str,
                      default='neo4j')
parse_io.add_argument('-p', '--password',
                      dest='password',
                      required=False,
                      type=str,
                      help='Password for neo4j database access. ')
parse_io.add_argument('-a', '--address',
                      dest='address',
                      required=False,
                      help='Address for neo4j database. ',
                      type=str,
                      default='bolt://localhost:7687')
parse_io.add_argument('-net', '--networks',
                      dest='networks',
                      required=False,
                      help='One or more network files. These can be graphml, gml or txt edge lists. ',
                      type=list,
                      default=None)
parse_io.add_argument('-del', '--delete',
                      dest='delete',
                      required=False,
                      help='If true, specify with the -net parameter names of networks (without full path) '
                           'to delete from the database. If no networks are mentioned, all are deleted.',
                      type=list,
                      default=None)
parse_io.add_argument('-fasta', '--fasta_sequences',
                      dest='fasta',
                      required=False,
                      help='FASTA files to add to database. ',
                      type=list,
                      default=None)
parse_io.add_argument('-cyto', '--cytoscape',
                      dest='cyto',
                      action='store_true',
                      help='If true and network names are given in the -net parameter, exports these '
                           'networks to Cytoscape. Otherwise, exports all networks. ',
                      required=False,
                      default=None)
parse_io.add_argument('-meta', '--metadata',
                      dest='meta',
                      required=False,
                      help='Locations of metadata, given as an edge list, to add to the database. ',
                      type=list,
                      default=None)
parse_io.add_argument('-w', '--write',
                      dest='write',
                      action='store_true',
                      required=False,
                      help='If true and network or set names are given, these are written to graphml files.'
                           'Otherwise, all networks and sets are exported. ',
                      default=None)


parse_netstats = subparsers.add_parser('netstats', description='Carry out analysis on the networks in the database.',
                                       help='The netstats module contains functions that carry out some '
                                            'form of analysis'
                                            ' on the networks that does not involve node metadata. '
                                            'For example, set operations and clustering can be '
                                            'carried out from this module. ')
parse_netstats.add_argument('-fp', '--output_filepath',
                            dest='fp',
                            help='File path for importing and / or exporting files. ',
                            default=None)
parse_netstats.add_argument('-cf', '--config',
                            dest='config',
                            action='store_true',
                            help='If true, store config files to reload Neo4j settings. ',
                            required=False,
                            default=None)
parse_netstats.add_argument('-u', '--username',
                            dest='username',
                            required=False,
                            help='Username for neo4j database access. ',
                            type=str,
                            default='neo4j')
parse_netstats.add_argument('-p', '--password',
                            dest='password',
                            required=False,
                            type=str,
                            help='Password for neo4j database access. ')
parse_netstats.add_argument('-a', '--address',
                            dest='address',
                            required=False,
                            help='Address for neo4j database. ',
                            type=str,
                            default='bolt://localhost:7687')
parse_netstats.add_argument('-net', '--networks',
                            dest='networks',
                            required=False,
                            help='If you only want to carry out set operations on specific networks, list them here. ',
                            type=list,
                            default=None)
parse_netstats.add_argument('-set', '--set_operation',
                            dest='set',
                            required=False,
                            help='If flagged, adds nodes linking associations to the union, difference and intersection. ',
                            action='store_true',
                            default=None)
parse_netstats.add_argument('-w', '--weight',
                            dest='weight',
                            required=False,
                            help='If flagged, edge weight is not taken into account for sets. ',
                            action='store_false',
                            default=True)
parse_netstats.add_argument('-frac', '--fraction',
                            dest='fraction',
                            required=False,
                            help='List of fractions to use for partial intersections. ',
                            type=list,
                            default=[1])

parse_metastats = subparsers.add_parser('metastats', description='Carry out analysis on the networks in the database.',
                                       help='The metastats module contains functions that carry out some '
                                            'form of analysis'
                                            ' on the networks that involve node metadata. '
                                            'For example, metadata assocations can be calculated with this module. ')
parse_metastats.add_argument('-fp', '--output_filepath',
                             dest='fp',
                             help='File path for importing and / or exporting files. ',
                             default=None)
parse_metastats.add_argument('-cf', '--config',
                             dest='config',
                             action='store_true',
                             help='If true, store config files to reload Neo4j settings. ',
                             required=False,
                             default=None)
parse_metastats.add_argument('-u', '--username',
                             dest='username',
                             required=False,
                             help='Username for neo4j database access. ',
                             type=str,
                             default='neo4j')
parse_metastats.add_argument('-p', '--password',
                             dest='password',
                             required=False,
                             type=str,
                             help='Password for neo4j database access. ')
parse_metastats.add_argument('-a', '--address',
                             dest='address',
                             required=False,
                             help='Address for neo4j database. ',
                             type=str,
                             default='bolt://localhost:7687')
parse_metastats.add_argument('-net', '--networks',
                             dest='networks',
                             required=False,
                             help='If you only want to carry out set operations on specific networks, list them here. ',
                             type=list,
                             default=None)


def main():
    mp.freeze_support()
    options = mako_parser.parse_args()
    mako(vars(options))


if __name__ == '__main__':
    main()
