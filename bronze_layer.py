import os
from dotenv import load_dotenv
from datetime import datetime as dt
from pyspark.sql.functions import lit

from scripts.utils import init_spark_session, define_logger
from scripts.extract import DataUsaApiExtractor
from scripts.load import DataUsaLoader



def build_api_urls(min_year:int=2010, max_year:int=2020) -> list[str] :
    """ """

    YEARS = range(min_year, max_year)
    urls = list(map(
            lambda year: f"https://api.datausa.io/tesseract/data.jsonrecords?cube=acs_yg_total_population_5&measures=Population&include=Year:{year}&drilldowns=State,Year",
            YEARS)
        )
    
    return urls


# Initialize Logger and constants
load_dotenv('.databricks.env')
RAW_DATA_TABLE = os.getenv('BRONZE_TABLE_NAME')
CURRENT_DATE = dt.now().date().isoformat()
bronze_logger = define_logger(os.path.join(os.getenv('PROJECT_DIR'), 'logs/bronze.log'))

# =================== HERE WE GO =================== #
bronze_logger.info('==== PROGRAM ON BRONZE LAYER STARTED =====')
SPARK_SESSION = init_spark_session()
bronze_logger.info('Spark Session initialized')

# Get urls of data from API
DATA_API_URLS = build_api_urls()
bronze_logger.info(f"Got {len(DATA_API_URLS)} api urls to extract")

# Get data from urls
extractor = DataUsaApiExtractor(spark_session=SPARK_SESSION, logger=bronze_logger)
df = extractor.extract_data(urls=DATA_API_URLS, headers={'user-agent' : os.getenv('USER_AGENT') })
bronze_logger.info(f"Data extracted and got {df.count()} rows")

# Additional column
df = df.withColumnRenamed('State ID', 'State_ID')
df = df.withColumn('Extracted_at', lit(CURRENT_DATE))

# Save data
data_loader = DataUsaLoader(dataframe=df,
                            spark_session=SPARK_SESSION,
                            logger=bronze_logger)
data_loader.save_as_table(table_fp=RAW_DATA_TABLE, mode='append')

bronze_logger.info('==== PROGRAM ON BRONZE LAYER FINISHED =====')