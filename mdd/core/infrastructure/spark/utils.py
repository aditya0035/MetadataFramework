from pyspark.sql import SparkSession
from pyspark import SparkFiles
import os
import json


def create_spark_session(app_name, master='local[*]', jar_packages=[], files=[], spark_config={}):
    spark_builder = (SparkSession.builder.master(master).appName(app_name))
    if jar_packages:
        spark_jar_packages = ",".join(jar_packages)
        spark_builder.config("spark.jars.packages", spark_jar_packages)

    if files:
        spark_files = '.'.join(files)
        spark_builder.config("spark.files", spark_files)

    if spark_config:
        for key, val in spark_config.items():
            spark_builder.config(key, val)
    spark_session = spark_builder.getOrCreate()
    spark_files_dir = SparkFiles.getRootDirectory()
    cfg_files = [filename for filename in os.listdir(spark_files_dir) if filename.endswith('config.json')]

    if cfg_files:
        path_to_cfg_file = os.path.join(spark_files_dir, cfg_files[0])
        with open(path_to_cfg_file, 'r') as cfg_files:
            cfg_dict = json.load(cfg_files)
    else:
        cfg_dict = None
    return spark_session, cfg_dict
