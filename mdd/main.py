import yaml
from mdd.core.coordinator import run
from mdd.core.infrastructure.spark.utils import create_spark_session
import os

if __name__ == "__main__":
    path = r"D:\Workspace\mdd\sample\csv_sample.yaml"
    with open(path, mode="rt") as fp:
        data = yaml.safe_load(fp.read())
        spark_config = {
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        }
        spark = create_spark_session("MddFramework", jar_packages=["io.delta:delta-core_2.12:2.2.0"],spark_config=spark_config)
        run(spark, data)
