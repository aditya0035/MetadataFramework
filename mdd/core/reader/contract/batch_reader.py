from mdd.core.reader.contract.base_reader import BaseReader
from abc import abstractmethod


class BatchReader(BaseReader):
    NAME = "batch_reader"

    @abstractmethod
    def read(self):
        pass
