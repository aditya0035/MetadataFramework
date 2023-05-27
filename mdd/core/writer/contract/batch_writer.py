from .base_writer import BaseWriter
from abc import abstractmethod


class BatchWriter(BaseWriter):
    NAME = "batch_writer"

    @abstractmethod
    def write(self):
        pass
