from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim
from functools import reduce
import logging


def count_data(dataframe: DataFrame) -> int :
    """ """

    total_rows = dataframe.count()
    logging.info(f"Total of rows in the dataframe : {total_rows}")
    
    return total_rows


def count_empty_values(dataframe: DataFrame, subsets: list[str]) -> int :
    """ """

    count = dataframe.filter(
        reduce(
            lambda col_1_condition, col_2_condition: col_1_condition | col_2_condition,
            [ col(column).isNull() | (trim(col(column)) == '') for column in subsets]
        )
    ).count()

    logging.info(f"Total of empty values based on Columns({subsets}) the dataframe : {count}")

    return count


def count_duplicates(dataframe: DataFrame, subsets: list[str]) -> int :
    """ """
    
    count = dataframe.groupBy(subsets) \
                     .count() \
                     .filter(col('count') > 1) \
                     .count()

    logging.info(f"Total of duplicated values based on Columns({subsets}) the dataframe : {count}")
    
    return count