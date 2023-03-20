from kafka import KafkaProducer, KafkaConsumer
import json
import os
import datetime
import requests
import logging
import uuid
import boto3
import backoff
from json.decoder import JSONDecodeError
from botocore.exceptions import ClientError
import io
import os
import time
import asyncio
import aiohttp
from threading import Thread
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import wait
from ast import Raise
from pprint import pprint
from typing import Dict, Tuple, Optional


"""
Asynchronous is a non-blocking architecture, so the execution of one task isn't dependent on another. Tasks can run simultaneously. Synchronous is a blocking architecture, so the execution of each operation is dependent on the completion of the one before it.

Coinbase API: https://docs.cloud.coinbase.com/sign-in-with-coinbase/docs/api-currencies
"""

# configure a handler to receive logging info
logging.getLogger('backoff').addHandler(logging.StreamHandler())

@backoff.on_predicate(backoff.constant, 
                      predicate=lambda r: r[0] == 429,
                      interval=0.5,
                      max_tries=15)
@backoff.on_exception(backoff.expo,
                      aiohttp.ClientResponseError,
                      max_time=60)
@backoff.on_exception(backoff.expo,
                      TimeoutError, 
                      max_time=300)
async def coinbase_response_generator(schema: str, next_url=None, currency_pair=None):
    """
    Generate URL and use aiohttp to obtain response asynchronously. 
    Return API response as a json, if exception occurs, raise and return status.
    """
    base_url = "https://api.coinbase.com/"
    
    if next_url:
        url = base_url + next_url
    elif next_url == None and schema == "currencies":
        url = f"{base_url}v2/currencies/crypto"
    elif next_url == None and schema == "prices_buy":
        url = f"{base_url}v2/prices/{currency_pair}/buy"
    elif next_url == None and schema == "prices_sell":
        url = f"{base_url}v2/prices/{currency_pair}/sell"

    try:
        async with aiohttp.ClientSession(raise_for_status=True) as session:
            async with session.get(url) as response:
                r_dict = await response.json()
                return response.status, r_dict
    
    except aiohttp.ClientResponseError as e:
        if e.status != None or e.status == 429:
            return e.status, None 
        else: raise e


def find(list_of_dicts, needed_key):
    """
    [
    {
        "code": "BTC",
        "name": "Bitcoin",
        "color": "#F7931A",
        "sort_index": 100,
        "exponent": 8,
        "type": "crypto",
        "address_regex": "^([13][a-km-zA-HJ-NP-Z1-9]{25,34})|^(bc1[qzry9x8gf2tvdw0s3jn54khce6mua7l]([qpzry9x8gf2tvdw0s3jn54khce6mua7l]{38}|[qpzry9x8gf2tvdw0s3jn54khce6mua7l]{58}))$",
        "asset_id": "5b71fc48-3dd3-540c-809b-f8c94d0e68b5"
    },
    {
        "code": "ETH",
        "name": "Ethereum",
        "color": "#627EEA",
        "sort_index": 102,
        "exponent": 8,
        "type": "crypto",
        "address_regex": "^(?:0x)?[0-9a-fA-F]{40}$",
        "asset_id": "d85dce9b-5b73-5c3c-8978-522ce1d1c1b4"
    },
    """
    for i in list_of_dicts:
        if i["id"] == needed_key:
            return i


async def get_child_data(schema_dict: dict, parent_endpoint: str, child_endpoint: str) -> Tuple:
    child_next_link = ''
    child_page_counter = 1
    child_schema_dict = {}

    for j in range(len(schema_dict[parent_endpoint])):
        currency_pair = schema_dict[parent_endpoint][j]["code"] + "-USD"
        
        while True:
            if not child_next_link:
                _, child_r_dict = await coinbase_response_generator(child_endpoint, currency_pair=currency_pair)
            elif child_next_link:
                _, child_r_dict = await coinbase_response_generator(child_endpoint, next_url=child_next_link, currency_pair=currency_pair)

            if child_endpoint not in child_schema_dict:
                child_schema_dict[child_endpoint] = child_r_dict

            if not child_r_dict:
                logging.info(f"Complete. Extracted all records for child endpoint {child_endpoint}. Total pages: {child_page_counter}.")
                break
            
            if child_endpoint in child_schema_dict:
                child_schema_dict[child_endpoint].extend(child_r_dict)

            child_r_dict["data"].extend({"currency_id": schema_dict[parent_endpoint][j]["code"]})

            logging.info(f"Extracted values for currency pair {currency_pair} of child endpoint {child_endpoint}, page {child_page_counter}.")

            try:
                child_next_link = child_r_dict["pagination"]["next_uri"]
                child_page_counter+=1
            except:
                logging.info(f"Complete. Extracted all records for child endpoints {child_endpoint}. Total pages: {child_page_counter}.")
                break

        return child_endpoint, child_schema_dict


async def coinbase_parent_generator(endpoints: tuple) -> dict:
    run_date = datetime.datetime.now(datetime.timezone.utc).isoformat()
    parent_endpoint, child_endpoints = endpoints
    next_link = ''
    page_counter = 1
    
    while True:
        schema_dict = {}

        if not next_link:
            _, r_dict = await coinbase_response_generator(parent_endpoint)
        elif next_link:
            _, r_dict = await coinbase_response_generator(parent_endpoint, next_url=next_link)

        if not r_dict:
            logging.info(f"Complete. Extracted all records for {parent_endpoint}.")
            break

        schema_dict[parent_endpoint] = r_dict

        for _, value in schema_dict.items():
            value["extract_timestamp"] = run_date

        logging.info(f"Extracted values for page {page_counter} of {parent_endpoint}.")

        coroutines = [get_child_data(schema_dict, parent_endpoint, child_endpoint) for child_endpoint in child_endpoints]
        child_results = await asyncio.gather(*coroutines) # Returns a list of Tuples
        
        #Find dictionary in the parent where ID matches currency_id in child
        for child_endpoint, child_endpoint_data in child_results:
            for j in schema_dict[parent_endpoint]["data"]:
                if child_endpoint_data["id"] == j["id"]:
                    j[child_endpoint] = child_endpoint_data["id"]
                                         
        pprint(schema_dict)

        yield schema_dict 

        try:
            next_link = r_dict["pagination"]["next_uri"]
            page_counter+=1
        except:
            logging.info(f"Complete. Extracted all records for {parent_endpoint}.")
            break


async def main():
    run_date = datetime.datetime.now(datetime.timezone.utc).isoformat()
    # session = aiohttp.ClientSession()
    endpoints = ("currencies", ["prices_buy", "prices_sell"])

    futures = coinbase_parent_generator((endpoints[0], endpoints[1]))
    # print(type(futures[0]))          
    async for future in futures:
        pprint(future)
    
    # await asyncio.gather(*futures)


def asyncio_run():
    asyncio.run(main())

asyncio_run()