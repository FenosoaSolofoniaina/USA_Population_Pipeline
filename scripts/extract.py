import requests
from typing import Any
import logging
from pyspark.sql import SparkSession,DataFrame



class DataUsaApiExtractor() :
    """ """


    def __init__(self, spark_session: SparkSession, logger: Any=None) -> None :
        """ """

        self.spark_session = spark_session
        self.logger = logger if logger else logging.getLogger(__name__)
        self.dataframe = None
    

    def send_requests(self, urls: list[str], headers: Any=None) -> list[Any] :
        """ """

        results = []

        for url in urls :
            try :
                response = requests.get(url=url, headers=headers)
                response.raise_for_status()
                jsonData = response.json()

                if len(jsonData['data']) :
                    results.extend(jsonData['data'])
                    self.logger.info(f"Got response from API '{url}' and had {len(jsonData['data'])} rows data")
                else :
                    self.logger.warning(f"Got response from API '{url}' but no data found")

            except requests.exceptions.RequestException as error :
                self.logger.error(f"An error occurred during sending requests to '{url}' : {error}")

            finally : continue

        return results

        
    def extract_data(self, urls: list[str], headers: Any=None) -> DataFrame :
        """ """

        raw_data = self.send_requests(urls=urls, headers=headers)
        df = self.spark_session.createDataFrame(raw_data)
        self.dataframe = df
        
        return df