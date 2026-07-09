import logging
from typing import Any, Union
from pyspark.sql import DataFrame, SparkSession



class DataUsaLoader() :
    """ """


    def __init__(self, spark_session: SparkSession, logger: Any=None, dataframe: Union[DataFrame, None]=None) -> None :
        """ """

        self.dataframe = dataframe
        self.spark_session = spark_session
        self.logger = logger if logger else logging.getLogger(__name__)


    def load_data_table(self, table_fp: str) -> DataFrame :
        """ """

        self.dataframe = self.spark_session.read.table(table_fp)
        self.logger.info(f'Data loaded from TABLE "{table_fp}"')

        return self.dataframe


    def save_to_csv(self, csv_fp: str, dataframe: Union[DataFrame, None]=None) -> None :
        """ """

        df = dataframe or self.dataframe
        df.toPandas().to_csv(csv_fp, index=False, header=True)
        self.logger.info(f'Data saved as CSV into "{csv_fp}"')


    def save_as_table(self, table_fp: str, mode: str='overwrite', dataframe: Union[DataFrame, None]=None) -> None :
        """ """

        df = dataframe or self.dataframe
        df.write.mode(mode).saveAsTable(table_fp)
        self.logger.info(f'Data saved as TABLE into "{table_fp}"')
