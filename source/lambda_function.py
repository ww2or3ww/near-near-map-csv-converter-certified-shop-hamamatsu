import sys
sys.dont_write_bytecode = True
import os
import os.path
import io
import csv
import json
import re
import requests
import pandas as pd
from retry import retry
from datetime import date, datetime
import numpy as np
import slackweb
import boto3
from boto3.dynamodb.conditions import Key

import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

API_ADDRESS                 = ""    if("API_ADDRESS" not in os.environ)             else os.environ["API_ADDRESS"]
DYNAMODB_NAME               = ""    if("DYNAMODB_NAME" not in os.environ)           else os.environ["DYNAMODB_NAME"]
DYNAMODB_KEY                = ""    if("DYNAMODB_KEY" not in os.environ)            else os.environ["DYNAMODB_KEY"]
S3_BUCKET_NAME              = ""    if("S3_BUCKET_NAME" not in os.environ)          else os.environ["S3_BUCKET_NAME"]
S3_PREFIX                   = ""    if("S3_PREFIX" not in os.environ)               else os.environ["S3_PREFIX"]
SLACK_WEBHOOK_HAMAMATSU     = ""    if("SLACK_WEBHOOK_HAMAMATSU" not in os.environ) else os.environ["SLACK_WEBHOOK_HAMAMATSU"]

DYNAMO_TABLE                = boto3.resource("dynamodb").Table(DYNAMODB_NAME)
S3_CLIENT                   = boto3.client("s3")

def lambda_handler(event, context):
    try:
        logger.info("--- START ---")
        logger.info(event)
        
        csv_data, csv_update = get_csv_data(get_api_address())
        last_update, last_data_count = getLastData()
        
        if csv_update == last_update:
            logger.info("not updated : {0}".format(csv_update))
            event["Result"] = 304
            return {
                "statusCode": 304,
                "body": "Not Modified"
            }
        
        file_path, data_count = convert_csv(csv_update, csv_data, last_data_count)
        total_count = last_data_count + data_count
        logger.info("csv update = {0}, data count = {1}".format(csv_update, data_count))
        
        if data_count > 0:
            upload_s3(file_path, S3_PREFIX, S3_BUCKET_NAME)
        
        message = "CSV CONVERTER : CERTIFIED SHOP HAMAMATSU\n{0}({1}) -> {2}({3}) (+{4})".format(
            last_update, last_data_count, csv_update, total_count, data_count)
        notifyToSlack(SLACK_WEBHOOK_HAMAMATSU, message)
        
        if last_update is None:
            insertItem(DYNAMODB_KEY, csv_update, total_count)
        else:
            updateItem(DYNAMODB_KEY, csv_update, total_count)

        event["Result"] = 200
        return {
            "statusCode": 200,
            "body": message
        }

    except Exception as e:
        logger.exception(e)
        return {
            "statusCode": 500,
            "body": "error"
        }
        
    finally:
        logger.info("--- FINALLY ---")

def get_api_address():
    # https://opendata.pref.shizuoka.jp/dataset/8282.html
    return API_ADDRESS
    
def get_csv_data(api_address):
    api_response = requests_with_retry(api_address).json()
    resources = api_response["result"]["resources"]
    csv_address, csv_update = get_csv_info_from_api_resources(resources)
    res = requests_with_retry(csv_address).content
    csv_data = pd.read_csv(io.StringIO(res.decode("shift-jis")), sep=",", engine="python")
    return csv_data, csv_update

@retry(tries=3, delay=1)
def requests_with_retry(address):
    return requests.get(address)

def get_csv_info_from_api_resources(resources):
    csv_address = None
    csv_update = None
    res_download_url = None
    for i in range(len(resources)):
        root, ext = os.path.splitext(resources[i]["download_url"])
        if ext.lower() == ".csv":
            csv_address = resources[i]["download_url"]
            logger.info(csv_address)
            # タイムゾーン変換 +09:00 -> +0900 for strptime %f
            date_str = resources[i]["updated"][:-3] + resources[i]["updated"][-2:]
            csv_update = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S.%f%z")
            csv_update = csv_update.strftime("%Y-%m-%dT%H-%M-%S")
            logger.info(csv_update)
            break
    return csv_address, csv_update

def getLastData():
    last_update = None
    last_data_count = 0
    record = selectItem(DYNAMODB_KEY)
    if record is None or record["Count"] is 0:
        return None, 0
    
    last_update = record["Items"][0]["value"]
    
    if "data1" in record["Items"][0]:
        last_data_count = int(record["Items"][0]["data1"])

    return last_update, last_data_count

@retry(tries=3, delay=1)
def selectItem(key):
    return DYNAMO_TABLE.query(
        KeyConditionExpression=Key("key").eq(key)
    )

@retry(tries=3, delay=1)
def insertItem(key, update, data_count):
    DYNAMO_TABLE.put_item(
      Item = {
        "key": key, 
        "value": update, 
        "data1": data_count
      }
    )

@retry(tries=3, delay=1)
def updateItem(key, update, data_count):
    DYNAMO_TABLE.update_item(
        Key={
            "key": key
        },
        UpdateExpression="set #value = :value, #data1 = :data1",
        ExpressionAttributeNames={
            "#value": "value", 
            "#data1": "data1"
        },
        ExpressionAttributeValues={
            ":value": update, 
            ":data1": data_count
        }
    )

@retry(tries=3, delay=1)
def upload_s3(file_path, prefix, bucket_name):
    name = os.path.basename(file_path)
    key = os.path.join(prefix, name)
    S3_CLIENT.upload_file(Filename = file_path, Bucket = bucket_name, Key = key)

@retry(tries=3, delay=1)
def notifyToSlack(webhook_url, text):
    slack = slackweb.Slack(url = webhook_url)
    slack.notify(text = text)

def convert_csv(csv_update, csv_data, last_data_count):
    try:
        data_count = 0
        local_file_path = "/tmp/hamamatsu_certified_{0}.csv".format(csv_update)
        with open(local_file_path, "w") as file:
            writer = csv.writer(file)
            writer.writerow(["type", "tel", "title", "address", "homepage", "facebook", "instagram", "twitter", "media1", "media2", "media3", "media4", "media5", "locoguide_id", "star"])

            tel_list = csv_data["店舗_電話番号"]
            title_list = csv_data["店舗_名称"]
            address_list = csv_data["店舗_所在地"]
            hp_list = csv_data["店舗_Webサイト"]
            kind_lsit = csv_data["店舗_業態_産業分類名"]
            
            for i in range(len(title_list)):
                if i < last_data_count:
                    continue
                data_count += 1
                data = {}
                kind = kind_lsit[i]
                if "喫茶店" in kind:
                    data["type"] = "drink"
                else:
                    data["type"] = "food"
                
                if tel_list[i] is not np.nan and tel_list[i] is not None and tel_list[i] is not "":
                    data["tel"] = re.sub("[()-]", "", tel_list[i])
                    
                data["title"] = re.sub("　", " ", title_list[i])
                data["address"] = address_list[i]
                data["homepage"] = hp_list[i]
                data["star"] = 1
                writeCsvLine(writer, data)
                
        return local_file_path, data_count
    
    except Exception as e:
        logger.exception(e)
        raise e

def writeCsvLine(writer, data):
    writer.writerow(
        [
            data["type"], 
            getContents(data, "tel"), 
            data["title"], 
            data["address"], 
            getContents(data, "homepage"), 
            getContents(data, "facebook"), 
            getContents(data, "instagram"), 
            getContents(data, "twitter"),
            getContents(data, "media1"), 
            getContents(data, "media2"), 
            getContents(data, "media3"), 
            getContents(data, "media4"), 
            getContents(data, "media5"),
            getContents(data, "locoguide_id"),
            getContents(data, "star")
        ])

def getContents(data, key):
    return data[key] if key in data and data[key] is not np.nan else ""
