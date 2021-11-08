#!/usr/bin/env bash

/opt/hive-metastore/bin/schematool -initSchema -dbType mysql
/opt/hive-metastore/bin/start-metastore
