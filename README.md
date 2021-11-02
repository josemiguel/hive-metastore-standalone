# Hive Metastore Standalone

_A dockerized infrastructure to build and keep running up a metastore without [Hive](https://hive.apache.org/)_


## Requirements and Installation

S3 pre-configuration

- Create a bucket for your warehouse, metadata of your databases, tables will be created in this bucket
- Replace value at metastore.warehouse.dir [conf/metastore-site.xml](https://github.com/josemiguel/hive-metastore-standalone/blob/main/conf/metastore-site.xml#L28)

## License
[Apache License 2.0](https://github.com/josemiguel/hive-metastore-standalone/LICENSE)
