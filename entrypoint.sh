#!/usr/bin/env bash

/opt/hive-metastore/bin/schematool -initSchema -dbType postgres
/opt/hive-metastore/bin/start-metastore
