from abc import ABC
from abc import ABCMeta, abstractmethod

class AbstractHiveEntity(metaclass=ABCMeta):
    def __init__(self, *args, **kwargs):
        pass

    @property
    def thrift_object(self):
        if self._thrift_object is None:
            self._thrift_object = self.get_thrift_object()
        return self._thrift_object

    @abstractmethod
    def get_thrift_object(self):
        pass