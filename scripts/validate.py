import logging
from typing import Any
from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim



class DataUsaApiValidator() :
    """ """


    def __init__(self, dataframe: DataFrame, logger: Any=None) -> None :
        """ """

        self.dataframe = dataframe
        self.logger = logger if logger else logging.getLogger(__name__)


    def count_data(self) -> int :
        """ """

        total_rows = self.dataframe.count()
        self.logger.info(f"Total of rows in the dataframe : {total_rows}")
        
        return total_rows


    def count_empty_values(self, subsets: list[str]) -> int :
        """ """

        count = self.dataframe.filter(
            reduce(
                lambda col_1_condition, col_2_condition: col_1_condition | col_2_condition,
                [ col(column).isNull() | (trim(col(column)) == '') for column in subsets]
            )
        ).count()

        self.logger.info(f"Total of empty values based on Columns({subsets}) the dataframe : {count}")

        return count


    def count_duplicates(self, subsets: list[str]) -> int :
        """ """
        
        count = self.dataframe.groupBy(subsets) \
                    .count() \
                    .filter(col('count') > 1) \
                    .count()

        self.logger.info(f"Total of duplicated values based on Columns({subsets}) the dataframe : {count}")
        
        return count