import logging
from pyspark.sql import SparkSession



def init_spark_session() -> SparkSession:
    """ """

    return SparkSession\
            .builder\
            .appName('USA_Population_ETL_Pipeline')\
            .getOrCreate()


def define_logger(file_log: str) -> logging.Logger :
    """ """

    logging.basicConfig(level=logging.INFO,
                        format='[%(asctime)s] [%(levelname)s] [%(filename)s %(funcName)s()] %(message)s',
                        datefmt='%Y-%M-%d %H:%M:%S',
                        filename=file_log,
                        filemode='w')
    logger = logging.getLogger('USA_Population_Pipeline_logger')

    return logger