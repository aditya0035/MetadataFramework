from abc import ABC, abstractmethod


class BaseStep(ABC):
    """
    Base Reader class for all the readers
    """
    NAME = "base_step"

    @classmethod
    def get_name(cls):
        return cls.NAME

    @abstractmethod
    def execute(self):
        pass
