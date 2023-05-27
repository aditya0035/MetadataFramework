from mdd.core.reader.contract.stream_reader import StreamReader
from pyspark.sql import SparkSession


class DeltaStreamReader(StreamReader):
    NAME = "delta_stream"

    def __init__(self, ctx, **kwargs):
        self.__ctx = ctx
        self.__path = kwargs.get("path", None)
        self.__ignore_deletes = kwargs.get("ignoreDeletes", None)
        self.__ignore_changes = kwargs.get("ignoreChanges", None)
        self.__starting_version = kwargs.get("startingVersion", None)
        self.__starting_timestamp = kwargs.get("startingTimestamp", None)
        self.__with_event_time_order = kwargs.get("withEventTimeOrder", None)
        self.__view_name = kwargs.get("view_name", None)
        self.__validate()

    def __validate(self):
        if not self.__view_name:
            raise Exception("view_name is mandatory for delta reader")
        if not self.__path:
            raise Exception("Path is mandatory for delta reader")
        if self.__starting_version and self.__starting_timestamp:
            raise Exception("Provide either startingVersion or startingTimestamp for delta reader not both")
        if self.__ignore_changes and self.__ignore_deletes:
            raise Exception("Provide either ignoreDeletes or ignoreChanges for delta reader not both")

    def read_stream(self):
        spark = SparkSession.getActiveSession()
        reader = spark.readStream.format("delta")
        if self.__ignore_deletes:
            reader = reader.option("ignoreDeletes", self.__ignore_deletes)
        if self.__ignore_changes:
            reader = reader.option("ignoreChanges", self.__ignore_changes)
        if self.__starting_version:
            reader = reader.option("startingVersion", self.__starting_version)
        if self.__starting_timestamp:
            reader = reader.option("startingTimestamp", self.__starting_timestamp)
        if self.__with_event_time_order:
            reader = reader.option("withEventTimeOrder", self.__with_event_time_order)
        reader_df = reader.load(self.__path)
        reader_df.createOrReplaceTempView(self.__view_name)
        return reader_df
