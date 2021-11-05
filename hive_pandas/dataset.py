from urllib.parse import urlparse

import boto3
from client.hive_metastore_client import HiveMetastoreClient

class HivePandasDataset():

  def __init__(self, hive_host=None, hive_port=9083, tablename=None, dataframe=None, schema=None):
    self.tablename = talename
    self.dataframe = dataframe
    self.schema = schema
    self.hive_host = hive_host
    self.hive_port = hive_port

  def write_csv(self, bucket, objectkey):
    s3client = boto3.client("s3")
    with io.StringIO() as csv_buffer:
        self.dataframe.to_csv(csv_buffer, index=False)

        response = s3client.put_object(Bucket=bucket, Key=objectkey, Body=csv_buffer.getvalue())
        response_metadata = response.get("ResponseMetadata", {})
        status = response_metadata.get("HTTPStatusCode")
        if status != 200:
            raise Exception(f"Unsuccessful S3 put_object response. Status - {status}")
        else:
            return response_metadata

  def save(self, location=None, partitions=None, fileformat='csv'):
    with HiveMetastoreClient(self.hive_host, self.hive_port) as hive_client:
      table = hive_client.get_table(self.tablename)
      if not location:
        if table is None:
          raise Exception("Table does not exists, you must pass variable `location`")
   
      else:
        if table is None:
          table = hive_client.create_table(tblname=self.tablename, loc=location, schema=self.schema, pts=partitions)

        parse_result = urlparse(location, allow_fragments=False)
        bucket = parse_result.netloc
        objectkey = parse_result.path.strip('/')
        partition_location_lst = []
        for key, value in partitions.items:
          partition_location_lst.append('{key}={value}'.format(key=key, value=value))

        objectkey += '/'.join(partition_location_lst)

        for index, df in enumerate([self.dataframe]):
          objectkey += 'raw_data_%08d.%s' % (index, fileformat)
        
        self.write_csv(bucket, objectkey)
        table.add_partition(partition)
