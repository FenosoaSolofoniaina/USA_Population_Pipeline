from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim, upper
from pyspark.sql.types import StructType
from functools import reduce
import logging


def remove_empty_values(dataframe: DataFrame, subsets: list[str]) -> DataFrame :
    """ """

    clean_df = dataframe.filter(
        reduce(
            lambda col_1_condition, col_2_condition: col_1_condition & col_2_condition,
            [ col(column).isNotNull() & (trim(col(column)) != '') for column in subsets]
        )
    )

    logging.info(f"Remove empty values based on Columns({subsets}). Keep {clean_df.count()} rows instead {dataframe.count()}")

    return clean_df


def remove_duplicates(dataframe: DataFrame, subsets: list[str]) -> DataFrame :
    """ """

    pass

    clean_df = dataframe.dropDuplicates(subset=subsets)

    logging.info(f"Remove duplicated values based on Columns({subsets}). Keep {clean_df.count()} rows instead {dataframe.count()}")

    return clean_df


def change_schema(dataframe: DataFrame, schema: StructType) -> DataFrame :
    """ """

    for field in schema.fields:
        dataframe = dataframe.withColumn(field.name, col(field.name).cast(field.dataType))

    logging.info(f"Change schema to : {dataframe.schema}")

    return dataframe


def to_uppercase(dataframe: DataFrame, subsets: list[str]) -> DataFrame :
    """ """

    for column in subsets :
        processed_df = dataframe.withColumn(column, upper(col(column)) )

    logging.info("TO UPPERCASE")

    return processed_df


def remove_white_space(dataframe: DataFrame, subsets: list[str]) -> DataFrame :
    """ """

    for column in subsets :
        processed_df = dataframe.withColumn( column, trim(col(column)) )

    logging.info("REMOVE WHITE SPACE")

    return processed_df