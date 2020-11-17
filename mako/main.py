"""
This file contains parsers and functions that call on other functionality defined
in the rest of mako's scripts directory.
The command line interface is intended to be called sequentially;
files are written to disk as intermediates,
while a settings file is used to transfer logs and other information
between the modules. These modules are contained in this file.
This modular design allows users to leave out parts of mako that are not required,
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
from pbr.version import VersionInfo
from mako.scripts.base import start_base
from mako.scripts.neo4biom import start_biom
from mako.scripts.io import start_io
from mako.scripts.netstats import start_netstats
from mako.scripts.metastats import start_metastats
from mako.scripts.wrapper import start_wrapper
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
    if mako_args['version']:
        info = VersionInfo('anuran')
        logger.info('Version ' + info.version_string())
        sys.exit(0)
    if 'base' in mako_args:
        logger.info('Running base Neo4j module. ')
        start_base(mako_args)
    if 'neo4biom' in mako_args:
        logger.info('Running Neo4biom module. ')
        start_biom(mako_args)
    if 'io' in mako_args:
        logger.info('Running IO module. ')
        start_io(mako_args)
    if 'netstats' in mako_args:
        logger.info('Performing network analysis on Neo4j database. ')
        start_netstats(mako_args)
    if 'metastats' in mako_args:
        logger.info('Performing metadata analysis on Neo4j database. ')
        start_metastats(mako_args)
    if 'manta' in mako_args:
        logger.info('Running manta on Neo4j database. ')
        start_wrapper(mako_args)
    if 'anuran' in mako_args:
        logger.info('Running anuran on Neo4j database. ')
        start_wrapper(mako_args)
    logger.info('Completed tasks! ')


mako_parser = argparse.ArgumentParser(description='mako pipeline')
mako_parser.add_argument('-s', '--set',
                         dest='settings',
                         help='Settings txt file containing '
                              'filepaths to processed data, '
                              'as well as other settings. ',
                         default=os.getcwd() + '\\config')
mako_parser.add_argument('-version', '--version',
                         dest='version',
                         required=False,
                         help='Version number.',
                         action='store_true',
                         default=False)
subparsers = mako_parser.add_subparsers(title="mako modules",
                                        description="Each module carries out a part of mako. "
                                                    "Modules can be used independently "
                                                    "as long as the correct settings file "
                                                    "is provided and the Neo4j instance is running. ")


def _add_standard_parser(parser):
    """
    Adds some standard arguments to the parsers.
    :param parser: Argparse parser
    :return:
    """
    parser.add_argument('-fp', '--output_filepath',
                        dest='fp',
                        help='File path for importing and / or exporting files. ',
                        default=os.getcwd())
    parser.add_argument('-cf', '--config',
                        dest='store_config',
                        action='store_true',
                        help='If true, store config files to reload Neo4j settings. ',
                        required=False,
                        default=None)
    parser.add_argument('-en', '--encryption',
                        dest='encryption',
                        action='store_true',
                        help='If flagged, the Neo4j database connection is encrypted. Not valid for Docker. ',
                        required=False,
                        default=False)
    parser.add_argument('-u', '--username',
                        dest='username',
                        required=False,
                        help='Username for neo4j database access. ',
                        type=str,
                        default='neo4j')
    parser.add_argument('-p', '--password',
                        dest='password',
                        required=False,
                        type=str,
                        help='Password for neo4j database access. ')
    parser.add_argument('-a', '--address',
                        dest='address',
                        required=False,
                        help='Address for neo4j database. ',
                        type=str,
                        default='bolt://localhost:7687')
    return parser


parse_base = subparsers.add_parser('base', description='Start, clear and quit the Neo4j database.',
                                   help='The base module runs the Neo4j console and carries out '
                                        'checks to validate the database consistency with the schema. '
                                        'These checks are especially valuable when you are manually '
                                        'editing the database.')
parse_base = _add_standard_parser(parse_base)
parse_base.add_argument('-n', '--neo4j',
                        dest='neo4j',
                        help='Filepath to neo4j folder. ',
                        required=False,
                        type=str,
                        default=None)
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
parse_base.set_defaults(base=True)


parse_neo4biom = subparsers.add_parser('neo4biom', description='Read/write operations for standard '
                                                           'microbial abundance data formats.',
                                       help='The neo4biom module contains functions that read BIOM files '
                                       'or tab-delimited files and write these to the Neo4j database. '
                                       'It can also delete files by referencing their file name. ')
parse_neo4biom = _add_standard_parser(parse_neo4biom)
parse_neo4biom.add_argument('-biom', '--biom_file',
                            dest='biom_file',
                            required=False,
                            help='One or more BIOM files. ',
                            type=str,
                            default=None,
                            nargs='+')
parse_neo4biom.add_argument('-count', '--count_table',
                            dest='count_table',
                            required=False,
                            help='One or more tab-delimited count tables. ',
                            type=str,
                            default=None,
                            nargs='+')
parse_neo4biom.add_argument('-tax', '--tax_table',
                            dest='tax_table',
                            required=False,
                            help='One or more tab-delimited taxonomy tables. ',
                            type=str,
                            default=None,
                            nargs='+')
parse_neo4biom.add_argument('-tm', '--taxon_metadata',
                            dest='taxon_meta',
                            required=False,
                            help='One or more tab-delimited taxon metadata tables. ',
                            type=str,
                            default=None,
                            nargs='+')
parse_neo4biom.add_argument('-sm', '--sample_metadata',
                            dest='sample_meta',
                            required=False,
                            help='One or more tab-delimited sample metadata tables. ',
                            type=str,
                            default=None,
                            nargs='+')
parse_neo4biom.add_argument('-del', '--delete',
                            dest='delete',
                            required=False,
                            help='Names of count tables (without full path) to delete from the database. ',
                            type=str,
                            default=None,
                            nargs='+')
parse_neo4biom.set_defaults(neo4biom=True)

parse_io = subparsers.add_parser('io', description='Read/write operations to disk, Cytoscape and Neo4j.',
                                 help='The io module contains functions that change the type of'
                                      ' data storage; for example, reading a network from an edge list to '
                                      'the Neo4j database, or exporting a Neo4j network to Cytoscape. '
                                      'To export to Cytoscape, you need to have started the software first. ')
parse_io = _add_standard_parser(parse_io)
parse_io.add_argument('-net', '--networks',
                      dest='networks',
                      required=False,
                      help='One or more network files. These can be graphml, gml or txt edge lists. ',
                      type=str,
                      default=None,
                            nargs='+')
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
                      type=str,
                      default=None,
                      nargs='+')
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
                      type=str,
                      default=None,
                      nargs='+')
parse_io.add_argument('-w', '--write',
                      dest='write',
                      action='store_true',
                      required=False,
                      help='If true and network or set names are given, these are written to graphml files.'
                           'Otherwise, all networks and sets are exported. ',
                      default=None)
parse_io.set_defaults(io=True)

parse_netstats = subparsers.add_parser('netstats', description='Carry out analysis on the networks in the database.',
                                       help='The netstats module contains functions that carry out some '
                                            'form of analysis'
                                            ' on the networks that does not involve node metadata. '
                                            'For example, set operations and clustering can be '
                                            'carried out from this module. ')
parse_netstats = _add_standard_parser(parse_netstats)
parse_netstats.add_argument('-net', '--networks',
                            dest='networks',
                            required=False,
                            help='If you only want to carry out set operations on specific networks, list them here. ',
                            type=str,
                            default=None,
                            nargs='+')
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
                            type=int,
                            default=[1],
                            nargs='+')
parse_netstats.set_defaults(netstats=True)

parse_metastats = subparsers.add_parser('metastats', description='Carry out analysis on the networks in the database.',
                                       help='The metastats module contains functions that carry out some '
                                            'form of analysis'
                                            ' on the networks that involve node metadata. '
                                            'For example, metadata assocations can be calculated with this module. ')
parse_metastats = _add_standard_parser(parse_metastats)

parse_metastats.add_argument('-agglom', '--agglomeration',
                             dest='agglom',
                             required=False,
                             help='Merges edges if edge taxa are identical at the specified level.',
                             type=str,
                             choices=['species', 'genus', 'family',
                                      'order', 'class', 'phylum'],
                             default=None)
parse_metastats.add_argument('-var', '--variable',
                             dest='variable',
                             required=False,
                             help='Spearman and hypergeometric test on sample-linked metadata to taxa. \n'
                                  'Specify "all" or one or more specific variable names.',
                             type=str,
                             default=None,
                             nargs='+')
parse_metastats.set_defaults(metastats=True)

parse_manta = subparsers.add_parser('manta', description='Cluster networks in the database.',
                                             help='The wrapper module can run manta and anuran \n '
                                                  'on networks extracted from the Neo4j database.')
parse_manta = _add_standard_parser(parse_manta)

parse_manta.add_argument('-net', '--networks',
                         dest='networks',
                         required=False,
                         help='If you only want to carry out set operations on specific networks, list them here. ',
                         type=str,
                         default=None,
                         nargs='+')
parse_manta.add_argument('-min', '--min_clusters',
                         dest='min',
                         required=False,
                         help='Minimum number of clusters. Default: 2.',
                         type=int,
                         default=2)
parse_manta.add_argument('-ms', '--min_size',
                         dest='min',
                         required=False,
                         help='inimum cluster size as fraction of network size divided by cluster number. Default: 0.2.',
                         type=float,
                         default=0.2)
parse_manta.add_argument('-max', '--max_clusters',
                         dest='max', type=int,
                         required=False,
                         help='Maximum number of clusters. Default: 4.',
                         default=4)
parse_manta.add_argument('-limit', '--convergence_limit',
                         dest='limit', type=float,
                         required=False,
                         help='The limit defines the minimum percentage decrease in error per iteration.'
                              ' If iterations do not decrease the error anymore, the matrix is considered converged. '
                              'Default: 2.',
                         default=2)
parse_manta.add_argument('-iter', '--iterations',
                         dest='iter', type=int,
                         required=False,
                         help='Number of iterations to repeat if convergence is not reached. Default: 20.',
                         default=20)
parse_manta.add_argument('-perm', '--permutation',
                         dest='perm', type=int,
                         required=False,
                         help='Number of permutation iterations for '
                              'network subsetting during partial iterations. Default: number of nodes.',
                         default=None)
parse_manta.add_argument('-subset', '--subset_fraction',
                         dest='subset', type=float,
                         required=False,
                         help='Fraction of edges that are used for subsetting'
                              ' if the input graph is not balanced. Default: 0.8.',
                         default=0.8)
parse_manta.add_argument('-ratio', '--stability_ratio',
                         dest='ratio', type=float,
                         required=False,
                         help='Fraction of scores that need to be positive or negative'
                              'for edge scores to be considered stable. Default: 0.8.',
                         default=0.8)
parse_manta.add_argument('-scale', '--edgescale',
                         dest='edgescale', type=float,
                         required=False,
                         help='Edge scale used to separate out weak cluster assignments. '
                              'The larger the edge scale, the larger the weak cluster. Default: 0.8.',
                         default=0.8)
parse_manta.add_argument('-cr, --cluster_reliability', dest='cr',
                         action='store_true',
                         default=False,
                         help='If flagged, reliability of cluster assignment is computed. ', required=False)
parse_manta.add_argument('-rel', '--reliability_permutations',
                         dest='rel', type=int,
                         required=False,
                         help='Number of permutation iterations for reliability estimates. \n '
                              'By default, this is 20. \n',
                         default=20)
parse_manta.add_argument('-e', '--error',
                         dest='error', type=int,
                         required=False,
                         help='Fraction of edges to rewire for reliability tests. Default: 0.1.',
                         default=0.1)
parse_manta.add_argument('-b', '--binary',
                         dest='bin',
                         action='store_true',
                         required=False,
                         default=False,
                         help='If flagged, edge weights are converted to 1 and -1. ')
parse_manta.set_defaults(manta=True)

parse_anuran = subparsers.add_parser('anuran', description='Analyse groups of networks in the database.',
                                               help='The wrapper module can run manta and anuran \n '
                                                    'on networks extracted from the Neo4j database.')
parse_anuran = _add_standard_parser(parse_anuran)

parse_anuran.add_argument('-net', '--networks',
                          dest='networks',
                          required=False,
                          help='If you only want to carry out set operations on specific networks, list them here. ',
                          type=str,
                          default=None,
                          nargs='+')
parse_anuran.add_argument('-size', '--intersection_size',
                          dest='size',
                          required=False,
                          nargs='+',
                          default=[1],
                          help='If specified, associations only shared by a number of networks '
                               'times the specified size fraction are included. \n'
                               'You can specify multiple numbers. By default, the full intersection is calculated.')
parse_anuran.add_argument('-sign', '--edge_sign',
                          dest='sign',
                          required=False,
                          help='If flagged, signs of edge weights are not taken into account. \n'
                               'The set difference then includes edges that have a unique edge sign in one network. \n'
                               'The set intersection then only includes edges that have the same sign across networks.',
                          default=True,
                          action='store_false')
parse_anuran.add_argument('-sample', '--resample',
                          dest='sample',
                          required=False,
                          type=int,
                          help='Resample your networks to observe the impact of increasing sample number. \n'
                               'when you increase the network number up until the total. \n'
                               'Specify an upper limit of resamples, or True if you want all possible resamples. \n'
                               'By default, the upper limit equal to the binomial coefficient of the input networks. \n'
                               'If the limit is higher than this coefficient, all possible combinations are resampled.',
                          default=False)
parse_anuran.add_argument('-n', '--sample_number',
                          dest='number',
                          required=False,
                          nargs='+',
                          default=None,
                          help='If you have a lot of samples, specify the sample numbers to test here. \n'
                               'For example: -n 4 8 12 will test the effect of acquiring 4, 8, and 12 samples. \n'
                               'By default, all sample numbers are tested.')
parse_anuran.add_argument('-cs', '--core_size',
                          dest='cs',
                          required=False,
                          nargs='+',
                          default=False,
                          help='If specified, true positive null models '
                               ' include a set fraction of shared interactions. \n'
                               'You can specify multiple fractions. '
                               'By default, null models have no shared interactions and '
                               'sets are computed for all randomized networks.\n. ')
parse_anuran.add_argument('-prev', '--core_prevalence',
                          dest='prev',
                          required=False,
                          nargs='+',
                          help='Specify the prevalence of the core. \n'
                               'By default, 1; each interaction is present in all models.',
                          default=[1])
parse_anuran.add_argument('-perm', '--permutations',
                          dest='perm',
                          type=int,
                          required=False,
                          help='Number of null models to generate for each input network. \n'
                               'Default: 10. ',
                          default=10)
parse_anuran.add_argument('-nperm', '--permutationsets',
                          dest='nperm',
                          required=False,
                          type=int,
                          help='Number of sets, centralities and graph values to calculate from the null models. \n'
                               'The total number of possible sets is equal to \n'
                               'the number of null models raised to the number of networks.\n '
                               'This value becomes huge quickly, so a random subset of possible sets is taken.\n '
                               'Default: 50. ',
                          default=50)
parse_anuran.add_argument('-c', '--centrality',
                          dest='centrality',
                          required=False,
                          action='store_true',
                          help='If true, extracts centrality ranking from networks \n'
                               'and compares these to rankings extracted from null models. ',
                          default=False)
parse_anuran.add_argument('-g', '--graph',
                          dest='graph',
                          required=False,
                          action='store_true',
                          help='If true, extracts network-level properties \n'
                               'and compares these to properties of randomized networks. ',
                          default=False)
parse_anuran.add_argument('-compare', '--compare_networks',
                          dest='comparison',
                          required=False,
                          help='If true, networks in the folders specified by the input parameter \n'
                               'are compared for different emergent properties. ',
                          default=False)
parse_anuran.add_argument('-draw', '--draw_figures',
                          dest='draw',
                          required=False,
                          help='If flagged, draws figures showing the set sizes.',
                          action='store_true',
                          default=False)
parse_anuran.add_argument('-stats', '--statistics',
                          dest='stats',
                          required=False,
                          help='Specify True or a multiple testing correction method to \n'
                               'calculate p-values for comparisons. \n'
                               'The available methods are listed in the docs for statsmodels.stats.multitest, \n'
                               'and include bonferroni, sidak, simes-hochberg, fdr_bh and others. ',
                          choices=['True', 'bonferroni', 'sidak', 'holm-sidak', 'holm',
                                   'simes-hochberg', 'hommel', 'fdr_bh', 'fdr_by',
                                   'fdr_tsbh', 'fdr_tsbky'],
                          default=False)
parse_anuran.add_argument('-core', '-processor_cores',
                          dest='core',
                          type=int,
                          required=False,
                          help='Number of processing cores to use. \n '
                               'By default, CPU count - 2. ',
                          default=os.cpu_count() - 2)
parse_anuran.set_defaults(anuran=True)


def main():
    mp.freeze_support()
    options = mako_parser.parse_args()
    mako(vars(options))


if __name__ == '__main__':
    main()
