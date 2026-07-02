import os
from dotenv import load_dotenv
from datetime import datetime as dt
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit

from scripts.utils import init_spark_session
from scripts.extract import send_requests
from scripts.load import save_as_table


def build_api_urls(min_year:int=2010, max_year:int=2020) -> list[str] :
    """ """

    YEARS = range(min_year, max_year)
    urls = list(map(
                lambda year: f"https://api.datausa.io/tesseract/data.jsonrecords?cube=acs_yg_total_population_5&measures=Population&include=Year:{year}&drilldowns=State,Year",
                YEARS)
            )
    
    return urls

    
def extract_data(spark_session: SparkSession,
                 urls: list[str]) -> DataFrame :
    """ """

    raw_data = send_requests(urls=urls)
    df = spark_session.createDataFrame(raw_data)
    
    return df


def ingest_bronze_layer(dataframe: DataFrame,
                        data_fp: str,
                        mode: str='append') -> None :
    """ """

    # EXTRACTION
    df = dataframe.withColumnRenamed('State ID', 'State_ID')
    CURRENT_DATE = dt.now().date().isoformat()
    df = df.withColumn('Extracted_at', lit(CURRENT_DATE))
    save_as_table(dataframe=df, fp=data_fp, mode=mode)


 # Initialize Logger and constants
load_dotenv()
RAW_DATA_TABLE = os.getenv('BRONZE_TABLE_NAME')

spark_session = init_spark_session()
urls = build_api_urls()
df = extract_data(spark_session, urls)
ingest_bronze_layer(df,
                    data_fp=RAW_DATA_TABLE)

print(f"DATA SAVED AT : '{RAW_DATA_TABLE}'")