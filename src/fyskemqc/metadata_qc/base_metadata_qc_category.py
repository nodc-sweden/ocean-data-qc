import abc

from fyskemqc.visit import Visit


class BaseMetadataQcCategory(abc.ABC):
    def __init__(self, visit: Visit):
        self._visit = visit

    @abc.abstractmethod
    def check(self): ...
