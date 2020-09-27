# Common utilities functions

A number of handy functions to improve the integration process between the multiple applicaitons/platforms. In addition to standard utilities functions (eg. logging, handling local settings, etc), the code also aims to:

1. Organize and manage data types and schemas in a consistent and reliable way. **Clearly defined data models and schemas is one of the fundational pieces of a robust and reliable data system.** However, depending on the database and library, data types are rarely consistently available and named, and schemas are almost always structured differently from one another (eg. availability of float vs double in Python vs Druid, naming of long vs bigint in ElasticSearch/Presto, etc.). As schemas evolve with a product's features, its important that these changes are persisted consistently acorss all platforms, and this is done by the code within this directory.

2. Package the most optimal methods of extracting and loading data from the various data stores for the ease of developing additional features. 