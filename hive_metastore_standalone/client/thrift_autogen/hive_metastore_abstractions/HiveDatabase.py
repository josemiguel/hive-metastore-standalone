
from hive_metastore_standalone.client.thrift_autogen.hive_metastore.ttypes import Database
from hive_metastore_standalone.client.thrift_autogen.hive_metastore_abstractions.AbstractHiveEntity import AbstractHiveEntity

class HiveDatabase(AbstractHiveEntity):
    def __init__(self, database_name=None, location=None):
        self.database_name = database_name
        self.location = location
        self._thrift_object = None

    def get_thrift_object(self):
        database = Database(
            name=self.database_name,
            description=None,
            locationUri=self.location,
            parameters=None,
            privileges=None,
            ownerName=None,
            ownerType=None,
            catalogName=None
        )
        return database

