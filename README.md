# _mako_ ![mako](https://github.com/ramellose/mako/blob/master/mako.png)
Microbial Associations Katalog. _mako_ helps you structure data from multiple networks to carry out better meta-analysis.
This package contains functions for importing BIOM and network files into a Neo4j database according to a strict database model.
Neo4j is a native graph database and therefore the natural match for linked biological data.
In addition to setting up the Neo4j database, _mako_ contains functions for exporting networks to Cytoscape through HTML.

By storing the data on a harddisk rather than in memory, _mako_ can store vast amounts of data compared to conventional network libraries.
This supports meta-analyses on much greater scales. Moreover, _mako_ allows you to carry out reasoning on your graph with plugins like [GraphScale](https://www.derivo.de/en/products/graphscale/).

_mako_ supports FAIR data by automatically generating a provenance log for your networks.
