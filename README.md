# _mako_ ![mako](https://github.com/ramellose/mako/blob/master/mako.png)

Microbial Associations Katalog. _mako_ helps you structure data from multiple networks to carry out better meta-analysis.
This package contains functions for importing BIOM and network files into a Neo4j database according to an OWL database model.
Neo4j is a native graph database and therefore the natural match for linked biological data.
In addition to setting up the Neo4j database, _mako_ contains functions for exporting networks to Cytoscape through HTML.

By storing the data on a harddisk rather than in memory, _mako_ can store vast amounts of data compared to conventional network libraries.
This supports meta-analyses on much greater scales. Moreover, _mako_ allows you to carry out reasoning on your graph with plugins like [GraphScale](https://www.derivo.de/en/products/graphscale/).

This API contains drivers for interacting with a Neo4j database.
Many of these drivers have functions that allow for BIOM files,
network files and other microbiome-related files to be ported to the database.

For more instructions on how to use mako and Neo4j, please take a look at the manual: [Neo4j for biologists](https://github.com/ramellose/mako/raw/master/docs/Neo4j_for_biologists.pdf).

Contact the author at lisa.rottjers (at) kuleuven.be. Your feedback is much appreciated!
This version is still in early alpha and has been tested for Python 3.6.

## Getting Started

First set up a [virtual environment](https://docs.python-guide.org/dev/virtualenvs/) and make sure it uses Python 3:
```
virtualenv venv
# Linux
source venv/bin/activate

# Windows
venv/Scripts/activate

# Once you are done with mako:
deactivate
```

To install _mako_, run:
```
pip install git+https://github.com/ramellose/mako.git
```

If you have both Python 2.7 and Python 3 installed, you may need to change the command to this:
```
python3 -m pip install git+https://github.com/ramellose/mako.git
```

You can run the _mako_ script and read the help docs with the following command.
To get more information for specific modules, please specify the module before _-h_.
More details are available in Chapter 6 of [Neo4j for biologists](https://github.com/ramellose/mako/raw/master/docs/Neo4j_for_biologists.pdf).

```
mako -h
```

For interacting with your Neo4j database, you will first need to start or connect to an instance of a Neo4j database.
Instructions on how to set up Neo4j can be found in Chapter 1 of [Neo4j for biologists](https://github.com/ramellose/mako/raw/master/docs/Neo4j_for_biologists.pdf). 
The _biom_ and _io_ modules allow you to upload BIOM files and networks respectively, or to write networks.
The _netstats_ module runs Neo4j queries to extract sets from specified networks.
The _metastats_ module can do some basic statistics, or agglomerate networks by taxonomic level.

### Contributions

This software is still in early alpha. Any feedback or bug reports will be much appreciated!

## Authors

* **Lisa Röttjers** - [ramellose](https://github.com/ramellose)

See also the list of [contributors](https://github.com/ramellose/manta/contributors) who participated in this project.

## License

This project is licensed under the Apache License - see the [LICENSE](LICENSE) file for details


