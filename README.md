# _mako_ ![mako](https://github.com/ramellose/mako/blob/master/mako.png)

[![HitCount](http://hits.dwyl.com/ramellose/mako.svg)](http://hits.dwyl.com/ramellose/mako)

Microbial Associations Katalog. _mako_ helps you structure data from multiple networks to carry out better meta-analysis.
This package contains functions for importing BIOM and network files into a Neo4j database according to an OWL database model.
Neo4j is a native graph database and therefore the natural match for linked biological data.
In addition to setting up the Neo4j database, _mako_ contains functions for exporting networks to Cytoscape through HTML.

By storing the data on a harddisk rather than in memory, _mako_ can store vast amounts of data compared to conventional network libraries.
This supports meta-analyses on much greater scales. Moreover, _mako_ allows you to carry out reasoning on your graph with plugins like [GraphScale](https://www.derivo.de/en/products/graphscale/).

This API contains drivers for interacting with a Neo4j database.
Many of these drivers have functions that allow for BIOM files,
network files and other microbiome-related files to be ported to the database.

For more instructions on how to use mako and Neo4j, please take a look at the homepage: [https://ramellose.github.io/mako_docs/](https://ramellose.github.io/mako_docs/).

Contact the author at lisa.rottjers (at) kuleuven.be. Your feedback is much appreciated!
This version is still in early alpha and has been tested for Python 3.6 and 3.8 on Ubuntu, OSX and Windows 10 systems. 

## Getting Started

You can use conda to install mako. 
First add the channel hosting mako as well as the bioconda channels: 
```
conda config --add channels bioconda
conda config --add channels conda-forge
conda config --add channels ramellose
```

Then create a new environment containing mako:
```
conda create -n myenv mako 
conda activate myenv
```

You can then call the mako command line tool from the conda environment. 

To install _mako_ locally, run:
```
python3 -m pip install git+https://github.com/ramellose/mako.git
```

You can run the _mako_ script and read the help docs with the following command.
To get more information for specific modules, please specify the module before _-h_.
More details are available on the [mako manual webpage](https://ramellose.github.io/mako_docs/manual/introduction/intro/).

```
mako -h
```

For interacting with your Neo4j database, you will first need to start or connect to an instance of a Neo4j database.
Instructions on how to set up Neo4j can be found on [the mako homepage](https://ramellose.github.io/mako_docs/neo4j/introduction/intro/). 
The _biom_ and _io_ modules allow you to upload BIOM files and networks respectively, or to write networks.
The _netstats_ module runs Neo4j queries to extract sets from specified networks.
The _metastats_ module can do some basic statistics, or agglomerate networks by taxonomic level.

### Contributions

This software is still in early alpha. Any feedback or bug reports will be much appreciated!

## Authors

* **Sam Röttjers** - [ramellose](https://github.com/ramellose)

See also the list of [contributors](https://github.com/ramellose/mako/contributors) who participated in this project.

## License

This project is licensed under the Apache License - see the [LICENSE](LICENSE) file for details


