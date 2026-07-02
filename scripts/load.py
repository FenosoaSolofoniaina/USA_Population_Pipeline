from pyspark.sql import DataFrame, SparkSession
import logging


def save_to_csv(dataframe: DataFrame, fp: str) -> None :
    """ """

    dataframe.toPandas().to_csv(fp, index=False, header=True)
    logging.info(f'Data saved into "{fp}"')


def save_as_table(dataframe: DataFrame, fp: str, mode: str='overwrite') -> None :
    """ """

    dataframe.write.mode(mode).saveAsTable(fp)
    logging.info(f'Data saved into "{fp}"')


def load_data_table(spark_session: SparkSession, fp: str) -> DataFrame :
    """ """

    df = spark_session.read.table(fp)
    logging.info(f'Data loaded from "{fp}"')

    return df