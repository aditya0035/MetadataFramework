from mdd.core.reader.runner import Runner as ReaderRunner
from mdd.core.transformer.runner import Runner as TransformationRunner
from mdd.core.writer.runner import Runner as WriterRunner


def run(spark, etl_configs):
    if type(etl_configs) is not dict:
        raise Exception("etl configurations should be a dict object")
    reader_configs = etl_configs.get("reader", None)
    if not reader_configs or type(reader_configs) is not dict:
        raise Exception("reader config should be provided and of dict type")
    transformation_config = etl_configs.get("transformation", None)
    if transformation_config and type(transformation_config) is not dict:
        raise Exception("transformation config should be of dict type")
    writer_config = etl_configs.get("writer", None)
    if not writer_config or type(writer_config) is not dict:
        raise Exception("writer config should be provided and of dict type")
    reader_runner = ReaderRunner(spark, reader_configs)
    transformation_runner = TransformationRunner(spark, transformation_config)
    writer_runner = WriterRunner(spark, writer_config)
    reader_runner.run()
    transformation_runner.run()
    writer_runner.run()
