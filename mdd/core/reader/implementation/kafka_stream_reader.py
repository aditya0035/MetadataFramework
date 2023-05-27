import json, pyspark.sql.functions as f
from pyspark.sql.types import *
from mdd.core.reader.contract.stream_reader import StreamReader
from pyspark.sql import SparkSession


class KafkaStreamReader(StreamReader):
    NAME = "kafka_stream"

    def __init__(self, ctx, **kwargs):
        self.__ctx = ctx
        self.__broker = kwargs.get("broker", None)
        self.__topic = kwargs.get("topic", None)
        self.__schema_file = kwargs.get("schema_file", None)
        self.__starting_offsets = kwargs.get("starting_offsets", "latest")
        self.__view_name = kwargs.get("view_name", None)
        self.__validate()

    def __validate(self):
        if not self.__view_name:
            raise Exception("view_name is mandatory for Kafka reader")
        if not self.__broker and not self.__topic:
            raise Exception("broker and topic is mandatory for Kafka reader")
        if not self.__schema_file:
            raise Exception("Schema file is mandatory for parsing")
        if self.__starting_offsets.lower() in ["latest", "earliest"]:
            raise Exception("Starting offset can be latest or earliest")

    def __get_schema_file_content(self):
        with open(self.__schema_file, "rt") as fp:
            content = fp.read()
        return json.loads(content)

    def read_stream(self):
        spark = SparkSession.getActiveSession()
        options = {
            "kafka.boostrap.servers": self.__broker,
            "subscribe": self.__topic,
            "failOnDataLoss": False,
            "startingOffsets": self.__starting_offsets
        }
        schema = self.__get_schema_file_content()
        reader_df = (spark
                     .readStream
                     .format("kafka")
                     .options(**options)
                     .load()
                     .selectExpr("cast(value as string)")
                     .select(f.from_json(f.col("value"), StructType.fromJson(schema)).alias("json"))
                     .select(f.col("json.*"))
                     )
        reader_df.createOrReplaceTempView(self.__view_name)
        return reader_df
