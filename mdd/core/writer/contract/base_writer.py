from abc import ABC


class BaseWriter(ABC):
    """
    Base Reader class for all the readers
    """
    NAME = "base_writer"

    @classmethod
    def get_name(cls):
        return cls.NAME
