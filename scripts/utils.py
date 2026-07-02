from pyspark.sql import SparkSession


def init_spark_session() -> SparkSession:
    """ """

    return SparkSession\
            .builder\
            .appName('ETL_DATA_USA_pipeline')\
            .getOrCreate()