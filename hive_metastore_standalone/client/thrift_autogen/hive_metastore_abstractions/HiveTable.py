import copy
from typing import Dict

from hive_metastore_standalone.client.thrift_autogen.hive_metastore.ttypes import SerDeInfo, FieldSchema, StorageDescriptor, Table, \
    PrincipalType, Partition
from hive_metastore_standalone.client.thrift_autogen.hive_metastore_abstractions.AbstractHiveEntity import AbstractHiveEntity


class HiveTable(AbstractHiveEntity):

    serde_type = {
        "csv":
            {"serde_lib": "org.apache.hadoop.hive.serde2.OpenCSVSerde",
             "serde_input": "org.apache.hadoop.mapred.TextInputFormat",
             "serde_output": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"},
        "parquet":
            {"serde_lib": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
             "serde_input": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
             "serde_output": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
             }
    }

    def __init__(self, database=None, tablename=None, location=None, schema=None, partitions=None, fileformat=None) -> None:
        self.database = database
        self.tablename = tablename
        self.location = location
        self.schema = schema
        self.partitions = partitions
        self.fileformat = fileformat

    @classmethod
    def from_thrift_object(cls, table_thrift_object: Table) -> "HiveTable":

        schema = {col.name: col.type for col in table_thrift_object.sd}
        partitions = {col.name: col.type for col in table_thrift_object.partitionKeys}


        hive_table = cls(database=table_thrift_object.dbName,
            tablename=table_thrift_object.tableName,
            location=table_thrift_object.sd.location,
            schema=schema,
            partitions=partitions,
            fileformat=None
            )

        fileformat = None
        for key, serde_info in hive_table.serde_type:
            if serde_info["serde_lib"] == table_thrift_object.sd.serdeInfo.serializationLib:
                fileformat = key

        if fileformat is None:
            raise Exception("Fileformat not found ")
        hive_table.fileformat = fileformat
        return hive_table

    def get_thrift_object(self):

        columns = []
        for col_name, col_type in self.schema.items():
            columns.append(FieldSchema(name=col_name, type=col_type, comment=""))

        partition_keys = []
        for part_name, part_type in self.partitions.items():
            partition_keys.append(FieldSchema(name=part_name, type=part_type, comment=""))

        serde_info = SerDeInfo(
            name=None,
            serializationLib=self.serde_type[self.fileformat]['serde_lib'],
            parameters=None,
            description=None,
            serializerClass=None,
            deserializerClass=None,
            serdeType=None
        )

        storage_descriptor = StorageDescriptor(
            cols=columns,
            location=self.location,
            inputFormat=self.serde_type[self.fileformat]['serde_input'],
            outputFormat=self.serde_type[self.fileformat]['serde_output'],
            compressed=None,
            numBuckets=None,
            serdeInfo=serde_info,
            bucketCols=None,
            sortCols=None,
            parameters=None,
            skewedInfo=None,
            storedAsSubDirectories=None
        )

        table = Table(
            tableName=self.tablename,
            dbName=self.database,
            owner="default owner",
            createTime=None,
            lastAccessTime=None,
            retention=None,
            sd=storage_descriptor,
            partitionKeys=partition_keys,
            parameters={"EXTERNAL": "TRUE"},
            viewOriginalText=None,
            viewExpandedText=None,
            tableType="EXTERNAL_TABLE",
            privileges=None,
            temporary=None,
            rewriteEnabled=None,
            creationMetadata=None,
            catName=None,
            ownerType=PrincipalType.USER
        )
        return table

    def get_partition_thrift_object(self, partition: Dict):
        table_thrift_object = self.thrift_object
        values = []
        location_suffix = []
        for part in table_thrift_object.partitionKeys:
            part_name = part.name
            part_value = partition[part_name]
            location_suffix.append(f"{part_name}={part_value}")
            values.append(part_value)

        table_storage_descriptor = table_thrift_object.sd
        partition_storage_descriptor = copy.deepcopy(table_storage_descriptor)
        partition_storage_descriptor.location += "/" + "/".join(location_suffix)

        new_partition_thrift_object = Partition(
            values=values,
            dbName=table_thrift_object.dbName,
            tableName=table_thrift_object.tableName,
            createTime=None,
            lastAccessTime=None,
            sd=partition_storage_descriptor,
            parameters=None,
            privileges=None,
            catName=None
        )
        return new_partition_thrift_object

    def add_columns(self, new_columns_schema):
        table = self.thrift_object #ref
        columns = []
        for col_name, col_type in new_columns_schema.items():
            columns.append(FieldSchema(name=col_name, type=col_type, comment=""))

        table.sd.cols.extend(columns)
        return table

    def drop_columns(self, old_columns_names):
        table = self.thrift_object #ref
        columns = []
        for col in table.sd.cols:
            if col.name not in old_columns_names:
                columns.append(col)

        table.sd.cols = columns
        return table
