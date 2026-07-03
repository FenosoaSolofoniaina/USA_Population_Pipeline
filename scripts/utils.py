from pyspark.sql import SparkSession


def init_spark_session() -> SparkSession:
    """ """

    return SparkSession\
            .builder\
            .appName('USA_Population_ETL_Pipeline')\
            .getOrCreate()