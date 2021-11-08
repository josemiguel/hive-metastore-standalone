from urllib.parse import urlparse
import io
import boto3
import logging
from client.hive_metastore_client import HiveMetastoreClient
from client.thrift_autogen.hive_metastore.ttypes import NoSuchObjectException, AlreadyExistsException
from client.thrift_autogen.hive_metastore_abstractions.HiveDatabase import HiveDatabase
from client.thrift_autogen.hive_metastore_abstractions.HiveTable import HiveTable
logging.basicConfig(level=logging.INFO)

class HivePandasDataset():

    def __init__(self, hive_host=None,
                 hive_port=9083,
                 database=None,
                 tablename=None,
                 dataframe=None,
                 schema=None,
                 partitions=None,
                 location=None):

        for col in list(dataframe.columns):
            if col not in schema and col not in partitions:
                raise Exception(f"Could not create Dataset schema mismatch {col} is not present in schema dict")

        self.database = database
        self.tablename = tablename
        self.dataframe = dataframe
        self.schema = schema
        self.hive_host = hive_host
        self.hive_port = hive_port
        self.partitions = partitions
        self.location = location

    def create_structures_if_not_exists(self):
        with HiveMetastoreClient(self.hive_host, self.hive_port) as hive_client:
            table_no_exists = False
            try:
                hive_database = HiveDatabase(self.database)
                hive_client.create_database(hive_database.thrift_object)
            except AlreadyExistsException:
                logging.info(f"Database {self.database} already exists")

            try:
                table = hive_client.get_table(self.database, self.tablename)
            except NoSuchObjectException:
                table_no_exists = True
                if not self.location:
                    raise Exception("Table does not exists, you must pass variable `location`")

            if table_no_exists:
                hive_table = HiveTable(database=self.database, tablename=self.tablename,
                                       location=self.location, schema=self.schema,
                                       partitions=self.partitions, fileformat='csv')

                table = hive_table.get_thrift_object()
                hive_client.create_table(hive_table.get_thrift_object())
            else:
                logging.info(f"Table {self.tablename} already exists")

            return table

    def validate_schema(self, sorted_columns, dataframe_cols):
        is_sorted = all([x == y for x, y in zip(sorted_columns, dataframe_cols)])
        equals = all([x == y for x, y in zip(sorted(sorted_columns), sorted(dataframe_cols))])

        if len(dataframe_cols) != len(sorted_columns):
            raise Exception(f"Number of columns different your data: {dataframe_cols} metastore: {sorted_columns}")
        if not equals:
            raise Exception(f"Name of columns different your data: {dataframe_cols} metastore: {sorted_columns}")

        if not is_sorted:
            self.dataframe = self.dataframe[sorted_columns]

    def save_as_csv(self, partition_values=None, merge_schema=False, overwrite=True):

        with HiveMetastoreClient(self.hive_host, self.hive_port) as hive_client:
            table = self.create_structures_if_not_exists(location)
            new_partition_thrift_object = HiveTable.get_partition_from_table_thrift_object(table, partition_values)

            sorted_columns = list(map(lambda col: col.name, table.sd.cols + table.partitionKeys))
            dataframe_cols = list(self.dataframe.columns)

            if not merge_schema:
                self.validate_schema(sorted_columns, dataframe_cols)
            else:
                new_columns = set(self.schema) - set(sorted_columns)  # (A,B,C) - (A, B) = C, new schema = ABC
                new_columns_schema = {k: v for k, v in self.schema.items() if k in new_columns}

                old_columns = set(sorted_columns) - set(self.schema)  # (B,C) - (B, A) = C, new schema = BA
                old_columns_names = [col for col in sorted_columns if col in old_columns]

                logging.info(f"Adding new columns:  {new_columns_schema.keys()} ")
                table = HiveTable.add_columns(table, new_columns_schema)
                logging.info(f"Removing old columns:  {old_columns_names} ")
                table = HiveTable.drop_columns(table, old_columns_names)

                hive_client.setMetaConf(hive_client.COL_TYPE_INCOMPATIBILITY_DISALLOW_CONFIG, "false")
                hive_client.alter_table(dbname=self.database, tbl_name=self.tablename, new_tbl=table)

            try:
                hive_client.add_partition(new_partition_thrift_object)

            except AlreadyExistsException:
                logging.info(f"Partition {new_partition_thrift_object.sd.location} already exists")

            finally:
                if overwrite:
                    logging.info(f"Overwritting data at {new_partition_thrift_object.sd.location}")
                    self.write_dataframe_csv(partition_location=new_partition_thrift_object.sd.location,
                                             overwrite=overwrite)

    def write_dataframe_csv(self, partition_location, overwrite=False):
        parse_result = urlparse(partition_location, allow_fragments=False)
        bucket = parse_result.netloc

        s3client = boto3.client("s3")
        objectkey = parse_result.path.strip('/')
        if overwrite:
            response = s3client.list_objects_v2(Bucket=bucket, Prefix=objectkey)

            for object in response.get('Contents', []):
                logging.info(f"Overwritting data: deleting: {object['Key']}")
                s3client.delete_object(Bucket=bucket, Key=object['Key'])

        for index, df in enumerate([self.dataframe]):
            objectkey = parse_result.path.strip('/')
            objectkey = objectkey + '/raw_data_%08d.%s' % (index, 'csv')
            with io.StringIO() as csv_buffer:
                self.dataframe.to_csv(csv_buffer, index=False, header=False)

                response = s3client.put_object(Bucket=bucket, Key=objectkey, Body=csv_buffer.getvalue())
                response_metadata = response.get("ResponseMetadata", {})
                status = response_metadata.get("HTTPStatusCode")
                if status != 200:
                    raise Exception(f"Unsuccessful S3 put_object response. Status - {status}")
                else:
                    logging.info(f"Writing data successfully: {objectkey}")
                    return response_metadata
