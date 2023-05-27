from mdd.core.writer.contract.stream_writer import StreamWriter
from pyspark.sql import SparkSession


class DeltaStreamWriter(StreamWriter):
    NAME = "delta_stream"

    def __init__(self, ctx, **kwargs):
        self.__ctx = ctx
        self.__path = kwargs.get("path", None)
        self.__mode = kwargs.get("mode", "overwrite")
        self.__partition_by = kwargs.get("partition_by", None)
        self.__write_view_name = kwargs.get("write_view_name", None)
        self.__validate()

    def __validate(self):
        if not self.__write_view_name:
            raise Exception("view_name is mandatory for delta reader")
        if not self.__path:
            raise Exception("Path is mandatory for delta reader")
        if type(self.__partition_by) != str or type(self.__write_view_name) != list:
            raise Exception(
                "Partition by should be either str type containing single column or a list containing multiple column")

    def write_stream(self):
        try:
            spark = SparkSession.getActiveSession()
            writer_df = spark.table(self.__write_view_name)
            writer = writer_df.write.format("delta").option("path", self.__path)
            if self.__partition_by == str:
                writer = writer.partitionBy(self.__partition_by)
            if self.__partition_by == list:
                writer = writer.partitionBy(*self.__partition_by)
            writer = writer.mode(self.__mode)
            writer.start()
        except Exception as e:
            raise
