from abc import ABC
from abc import ABCMeta, abstractmethod

class AbstractHiveEntity(metaclass=ABCMeta):
    def __init__(self, *args, **kwargs):
        pass

    @abstractmethod
    def get_thrift_object(self):
        pass