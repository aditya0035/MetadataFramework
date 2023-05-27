from mdd.core.reader.contract.batch_reader import BatchReader
from pyspark.sql import SparkSession


class CsvBatchReader(BatchReader):
    NAME = "csv_batch"

    def __init__(self, ctx, **kwargs):
        self.__ctx = ctx
        self.__path = kwargs.get("path", None)
        self.__seperator = kwargs.get("seperator", ",")
        self.__quote = kwargs.get("quote", "\"")
        self.__header = kwargs.get("header", False)
        self.__infer_schema = kwargs.get("infer_schema", False)
        self.__schema = kwargs.get("schema", None)
        self.__view_name = kwargs.get("view_name", None)
        self.__validate()

    def __validate(self):
        if not self.__view_name:
            raise Exception("view_name is mandatory for csv reader")
        if not self.__path:
            raise Exception("Path is mandatory for csv reader")
        if not self.__infer_schema and self.__schema:
            raise Exception("Provide either inferSchema as True or Provide Schema String for csv reader")

    def read(self):
        spark = SparkSession.getActiveSession()
        reader = (spark
                  .read.format("csv")
                  .option("sep", self.__seperator)
                  .option("quote", self.__quote)
                  .option("header", self.__header)
                  .option("inferSchema", self.__infer_schema)
                  )
        if self.__schema:
            reader = reader.option("schema", self.__schema)
        reader_df = reader.load(self.__path)
        reader_df.createOrReplaceTempView(self.__view_name)
        return reader_df
