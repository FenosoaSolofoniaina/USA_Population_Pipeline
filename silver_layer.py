import os
from dotenv import load_dotenv
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from pyspark.sql.functions import col, max

from scripts.utils import init_spark_session, define_logger
from scripts.validate import DataUsaApiValidator
from scripts.transform import DataUsaColumnTransformer, DataUsaRowTransformer
from scripts.load import DataUsaLoader


# Initialize Logger and constants
load_dotenv('.databricks.env')
silver_logger = define_logger(os.path.join(os.getenv('PROJECT_DIR'), 'logs/silver.log'))
RAW_DATA_FP = os.getenv('BRONZE_TABLE_NAME')
CLEAN_DATA_TABLE = os.getenv('SILVER_TABLE_NAME')
HISTORICAL_DATA_TABLE = os.getenv('SILVER_HISTORICAL_TABLE_NAME')

# =================== HERE WE GO =================== #
silver_logger.info('==== PROGRAM ON SILVER LAYER STARTED =====')
SPARK_SESSION = init_spark_session()
silver_logger.info('Spark Session initialized')

# Load data
data_loader = DataUsaLoader(spark_session=SPARK_SESSION, logger=silver_logger)
df = data_loader.load_data_table(table_fp=RAW_DATA_FP)

# CHECK DATA QUALITY
validator = DataUsaApiValidator(dataframe=df, logger=silver_logger)
not_empty_columns = ['State_ID', 'Year', 'Population']
unique_columns = ['State_ID', 'Year', 'Extracted_at']
total_rows = validator.count_data()
total_empty_values = validator.count_empty_values(subsets=not_empty_columns)
total_duplicates = validator.count_duplicates(subsets=unique_columns)

# Clean column
column_transformer = DataUsaColumnTransformer(dataframe=df, logger=silver_logger)
if total_rows :
    if total_empty_values :
        column_transformer.remove_empty_values(subsets=not_empty_columns)
    
    if total_duplicates :
        column_transformer.remove_duplicates(subsets=unique_columns)

# Transform schema
df = column_transformer.change_schema(
        schema=StructType([
                StructField(name='State_ID', dataType=StringType(), nullable=False),
                StructField(name='State', dataType=StringType(), nullable=False),
                StructField(name='Year', dataType=IntegerType(), nullable=False),
                StructField(name='Population', dataType=IntegerType(), nullable=False),
                StructField(name='Extracted_at', dataType=DateType(), nullable=False)
            ])
    )

# Clean each row
text_columns = ['State_ID', 'State']
row_transformer = DataUsaRowTransformer(dataframe=df, logger=silver_logger)
df = row_transformer.to_uppercase(subsets=text_columns)
df = row_transformer.remove_white_space(subsets=text_columns)

# SAVE HISTORICAL DATA
data_loader.save_as_table(dataframe=df, table_fp=HISTORICAL_DATA_TABLE, mode='overwrite')
silver_logger.info(f"HISTORICAL DATA SAVED AT : '{HISTORICAL_DATA_TABLE}'")

# Keep the latest version for gold layer
df = df.filter( col('Extracted_at') == df.select(max('Extracted_at')).first()[0] )

# SAVE DATA
data_loader.save_as_table(dataframe=df, table_fp=CLEAN_DATA_TABLE, mode='overwrite')
silver_logger.info(f"CLEANED DATA SAVED AT : '{CLEAN_DATA_TABLE}'")

silver_logger.info('==== PROGRAM ON SILVER LAYER FINISHED =====')