from mdd.core.transformer.contract.base_step import BaseStep
from pyspark.sql import SparkSession


class SqlStep(BaseStep):
    NAME = "sql_step"

    def __init__(self, ctx, **kwargs):
        self.__ctx = ctx
        self.__query = kwargs.get("query", None)
        self.__view_name = kwargs.get("view_name", None)

    def execute(self):
        try:
            spark = SparkSession.getActiveSession()
            transformed_df = spark.sql(self.__query)
            transformed_df.createOrReplaceTempView(self.__view_name)
        except Exception as e:
            raise
