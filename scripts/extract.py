import os
import requests
from typing import Any
import logging


def send_requests(urls: list[str], headers: Any=None) -> list[Any] :
    """ """

    results = []

    for url in urls :

        try :
            response = requests.get(url=url, headers=headers)
            response.raise_for_status()
            jsonData = response.json()
            if len(jsonData['data']) :
                results.extend(jsonData['data'])
                logging.info(f"Got response from API '{url}' and had {len(jsonData['data'])} rows data")

            else :
                logging.warning(f"Got response from API '{url}' but no data found")

        except requests.exceptions.RequestException as error :
            logging.error(f"An error occurred during sending requests to '{url}' : {error}")

        finally :
            continue


    return results