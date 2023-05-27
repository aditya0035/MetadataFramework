from .batch_writer import BaseWriter
from abc import abstractmethod


class StreamWriter(BaseWriter):
    NAME = "stream_reader"

    @abstractmethod
    def write_stream(self):
        pass
