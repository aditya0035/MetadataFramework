from mdd.core.writer.context import Context
from mdd.core.writer.contract.batch_writer import BatchWriter
from mdd.core.writer.contract.stream_writer import StreamWriter
import os
from glob import glob
import importlib.util
import sys
import inspect


class Runner:

    def __init__(self, spark, config):
        self.__spark = spark
        self.__config = config
        self.__source_mapping = Runner.create_mapping()

    def run(self):
        with Context(self.__spark) as ctx:
            for source_identifier, source_config in self.__config.items():
                source_name = source_config.get("type", None)
                source_type = source_name.split("_")[-1]
                sources = self.__source_mapping.get(source_type, None)
                if sources:
                    source = sources.get(source_name, None)
                    if source:
                        source_obj = source(ctx, **source_config)
                        if issubclass(source, BatchWriter):
                            source_obj.write()
                        elif issubclass(source, StreamWriter):
                            source_obj.write_stream()
                    else:
                        raise Exception(f"{source_type} is not defined")
                else:
                    raise Exception("No Writer is registered into batch and stream.Please create some implementation")

    @staticmethod
    def create_mapping():
        source_mapping = {'batch': {}, 'stream': {}}
        for file in glob(os.path.join(os.path.dirname(os.path.abspath(__file__)), "implementation", "*.py")):
            name = os.path.splitext(os.path.basename(file))[0]
            spec = importlib.util.spec_from_file_location(name, file)
            module = importlib.util.module_from_spec(spec)
            sys.modules[name] = module
            spec.loader.exec_module(module)
            for name, obj in inspect.getmembers(module,
                                                lambda cls: inspect.isclass(cls) and not inspect.isabstract(cls)):
                if issubclass(obj, BatchWriter):
                    source_type = obj.get_name()
                    source_mapping['batch'][source_type] = obj
                elif issubclass(obj, StreamWriter):
                    source_type = obj.get_name()
                    source_mapping['stream'][source_type] = obj
        return source_mapping
