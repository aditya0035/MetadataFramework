from mdd.core.reader.contract.batch_reader import BatchReader
from pyspark.sql import SparkSession


class DeltaBatchReader(BatchReader):
    NAME = "delta_batch"

    def __init__(self, ctx, **kwargs):
        self.__ctx = ctx
        self.__path = kwargs.get("path", None)
        self.__version_as_of = kwargs.get("versionAsOf", None)
        self.__timestamp_as_of = kwargs.get("timestampAsOf", None)
        self.__view_name = kwargs.get("view_name", None)
        self.__validate()

    def __validate(self):
        if not self.__view_name:
            raise Exception("view_name is mandatory for delta reader")
        if not self.__path:
            raise Exception("Path is mandatory for delta reader")
        if self.__version_as_of and self.__timestamp_as_of:
            raise Exception("Provide either versionAsOf or timestampAsOf for delta reader not both")

    def read(self):
        spark = SparkSession.getActiveSession()
        reader = spark.read.format("delta")
        if self.__version_as_of:
            reader = reader.option("versionAsOf", self.__version_as_of)
        if self.__timestamp_as_of:
            reader - reader.option("timestampAsOf", self.__timestamp_as_of)
        reader_df = reader.load(self.__path)
        reader_df.createOrReplaceTempView(self.__view_name)
        return reader_df
