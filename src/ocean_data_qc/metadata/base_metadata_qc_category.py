import abc

from ocean_data_qc.metadata.visit import Visit


class BaseMetadataQcCategory(abc.ABC):
    def __init__(self, visit: Visit):
        self._visit = visit

    @abc.abstractmethod
    def check(self): ...
