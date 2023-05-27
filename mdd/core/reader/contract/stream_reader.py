from mdd.core.reader.contract.base_reader import BaseReader
from abc import abstractmethod


class StreamReader(BaseReader):
    __NAME = "stream_reader"

    @abstractmethod
    def read_stream(self):
        pass
