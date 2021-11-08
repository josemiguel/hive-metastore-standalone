
from client.thrift_autogen.hive_metastore.ttypes import Database
from client.thrift_autogen.hive_metastore_abstractions.AbstractHiveEntity import AbstractHiveEntity

class HiveDatabase(AbstractHiveEntity):
    def __init__(self, database_name=None):
        self.database_name = database_name
        self._thrift_object = None

    def get_thrift_object(self):
        database = Database(
            name=self.database_name,
            description=None,
            locationUri=None,
            parameters=None,
            privileges=None,
            ownerName=None,
            ownerType=None,
            catalogName=None
        )
        return database

