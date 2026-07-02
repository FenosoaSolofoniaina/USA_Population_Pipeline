import os
from dotenv import load_dotenv
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from pyspark.sql.functions import col, max

from scripts.utils import init_spark_session
from scripts.validate import count_data, count_empty_values, count_duplicates
from scripts.transform import remove_empty_values, remove_duplicates
from scripts.transform import change_schema, to_uppercase, remove_white_space
from scripts.load import load_data_table, save_as_table


load_dotenv()
spark_session = init_spark_session()
RAW_DATA_FP = os.getenv('BRONZE_TABLE_NAME')
CLEAN_DATA_TABLE = os.getenv('SILVER_TABLE_NAME')
HISTORICAL_DATA_TABLE = os.getenv('SILVER_HISTORICAL_TABLE_NAME')
df = load_data_table(spark_session, fp=RAW_DATA_FP)

# CHECK DATA QUALITY
not_empty_columns = ['State_ID', 'Year', 'Population']
unique_columns = ['State_ID', 'Year', 'Extracted_at']
total_rows = count_data(df)
total_empty_values = count_empty_values(df, subsets=not_empty_columns)
total_duplicates = count_duplicates(df, subsets=unique_columns)

if total_rows :
    if total_empty_values :
        df = remove_empty_values(df, subsets=not_empty_columns)
    
    if total_duplicates :
        df = remove_duplicates(df, subsets=unique_columns)

# TRANSFORMATION
df = change_schema(df,
                    schema=StructType([
                            StructField(name='State_ID', dataType=StringType(), nullable=False),
                            StructField(name='State', dataType=StringType(), nullable=False),
                            StructField(name='Year', dataType=IntegerType(), nullable=False),
                            StructField(name='Population', dataType=IntegerType(), nullable=False),
                            StructField(name='Extracted_at', dataType=DateType(), nullable=False)
                        ])
    )

# CLEANING
text_columns = ['State_ID', 'State']
df = to_uppercase(df, subsets=text_columns)
df = remove_white_space(df, subsets=text_columns)

# SAVE HISTORICAL DATA
save_as_table(dataframe=df, fp=HISTORICAL_DATA_TABLE, mode='overwrite')
print(f"HISTORICAL DATA SAVED AT : '{HISTORICAL_DATA_TABLE}'")

# Keep the latest version for gold layer
df = df.filter( col('Extracted_at') == df.select(max('Extracted_at')).first()[0] )

# SAVE DATA
save_as_table(dataframe=df, fp=CLEAN_DATA_TABLE, mode='overwrite')
print(f"DATA SAVED AT : '{CLEAN_DATA_TABLE}'")