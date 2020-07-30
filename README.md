# _mako_ ![mako](https://github.com/ramellose/mako/blob/master/mako.png)

Microbial Associations Katalog. _mako_ helps you structure data from multiple networks to carry out better meta-analysis.
This package contains functions for importing BIOM and network files into a Neo4j database according to a strict database model.
Neo4j is a native graph database and therefore the natural match for linked biological data.
In addition to setting up the Neo4j database, _mako_ contains functions for exporting networks to Cytoscape through HTML.

By storing the data on a harddisk rather than in memory, _mako_ can store vast amounts of data compared to conventional network libraries.
This supports meta-analyses on much greater scales. Moreover, _mako_ allows you to carry out reasoning on your graph with plugins like [GraphScale](https://www.derivo.de/en/products/graphscale/).

_mako_ supports FAIR data by automatically generating a provenance log for your networks.

This API contains drivers for interacting with a Neo4j database.
Many of these drivers have functions that allow for BIOM files,
network files and other microbiome-related files to be ported to the database.

Contact the author at lisa.rottjers (at) kuleuven.be. Your feedback is much appreciated!
This version is still in early beta and has been tested for Python 3.6.

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

### Contributions

This software is still in early alpha. Any feedback or bug reports will be much appreciated!

## Authors

* **Lisa Röttjers** - [ramellose](https://github.com/ramellose)

See also the list of [contributors](https://github.com/ramellose/manta/contributors) who participated in this project.

## License

This project is licensed under the Apache License - see the [LICENSE.txt](LICENSE.txt) file for details


