#!/usr/bin/env bash

/opt/hive-metastore/bin/schematool -initSchema -dbType mysql 2>&1 > /var/log/metastore_setup.log
/opt/hive-metastore/bin/start-metastore 2>&1 /var/log/metastore.log
