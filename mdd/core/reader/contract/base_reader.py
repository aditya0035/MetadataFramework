from abc import ABC


class BaseReader(ABC):
    """
    Base Reader class for all the readers
    """
    NAME = "base_reader"

    @classmethod
    def get_name(cls):
        return cls.NAME
