import copy
from typing import List, Any, Tuple, Dict

from thrift.protocol import TBinaryProtocol
from thrift.transport import TSocket, TTransport

from client.thrift_autogen.hive_metastore import ThriftHiveMetastore

class HiveMetastoreClient(ThriftHiveMetastore.Client):

    COL_TYPE_INCOMPATIBILITY_DISALLOW_CONFIG = (
        "hive.metastore.disallow.incompatible.col.type.changes"
    )

    def __init__(self, host: str, port: int = 9083) -> None:
        protocol = self._init_protocol(host, port)
        super().__init__(protocol)

    @staticmethod
    def _init_protocol(host: str, port: int) -> TBinaryProtocol:
        transport = TSocket.TSocket(host, int(port))
        transport = TTransport.TBufferedTransport(transport)

        return TBinaryProtocol.TBinaryProtocol(transport)

    def open(self) -> "HiveMetastoreClient":
        self._oprot.trans.open()
        return self

    def close(self) -> None:
        self._oprot.trans.close()

    def __enter__(self) -> "HiveMetastoreClient":
        self.open()
        return self

    def __exit__(self, exc_type: str, exc_val: str, exc_tb: str) -> None:
        self.close()

