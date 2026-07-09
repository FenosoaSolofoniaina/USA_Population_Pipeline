import logging
from typing import Any
from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim, upper
from pyspark.sql.types import StructType



class DataUsaColumnTransformer() :
    """ """


    def __init__(self, dataframe: DataFrame, logger: Any=None) -> None :
        """ """

        self.dataframe = dataframe
        self.logger = logger if logger else logging.getLogger(__name__)


    def change_schema(self, schema: StructType) -> DataFrame :
        """ """

        for field in schema.fields:
            self.dataframe = self.dataframe.withColumn(field.name, col(field.name).cast(field.dataType))

        logging.info(f"Change schema to : {self.dataframe.schema}")

        return self.dataframe
    
    
    def remove_duplicates(self, subsets: list[str]) -> DataFrame :
        """ """

        old_n_rows = self.dataframe.count()
        self.dataframe = self.dataframe.dropDuplicates(subset=subsets)

        self.logger.info(f"Remove duplicated values based on Columns({subsets}). Keep {self.dataframe.count()} rows instead {old_n_rows}")

        return self.dataframe


    def remove_empty_values(self, subsets: list[str]) -> DataFrame :
        """ """

        old_n_rows = self.dataframe.count()
        self.dataframe = self.dataframe.filter(
            reduce(
                lambda col_1_condition, col_2_condition: col_1_condition & col_2_condition,
                [ col(column).isNotNull() & (trim(col(column)) != '') for column in subsets]
            )
        )

        self.logger.info(f"Remove empty values based on Columns({subsets}). Keep {self.dataframe.count()} rows instead {old_n_rows}")

        return self.dataframe



class DataUsaRowTransformer() :
    """ """


    def __init__(self, dataframe: DataFrame, logger: Any=None) -> None :
        """ """

        self.dataframe = dataframe
        self.logger = logger if logger else logging.getLogger(__name__)


    def to_uppercase(self, subsets: list[str]) -> DataFrame :
        """ """

        for column in subsets :
            self.dataframe = self.dataframe.withColumn(column, upper(col(column)) )

        self.logger.info("TO UPPERCASE")

        return self.dataframe


    def remove_white_space(self, subsets: list[str]) -> DataFrame :
        """ """

        for column in subsets :
            self.dataframe = self.dataframe.withColumn( column, trim(col(column)) )

        self.logger.info("REMOVE WHITE SPACE")

        return self.dataframe