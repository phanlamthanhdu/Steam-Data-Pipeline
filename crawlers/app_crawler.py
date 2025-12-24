import boto3
import json
import yaml
import random
import requests

http_proxies = [line.strip() for line in open(file = "proxies.txt", mode = 'r').readlines()]

s3 = boto3.client("s3")

s3_configs = yaml.safe_load(open(file = "configs/aws.yaml", mode = "r").read())
tag_dir = {
    "bucket": s3_configs['s3']['bucket'],
    "endpoint": f"{s3_configs['s3']['raw_prefix']}/{s3_configs['s3']['tag_endpoint']}",
    "file_name": s3_configs['s3']['tag_filename']
}
app_dir = {
    "bucket": s3_configs['s3']['bucket'],
    "endpoint": f"{s3_configs['s3']['raw_prefix']}/{s3_configs['s3']['app_endpoint']}",
    "file_name": s3_configs['s3']['app_filename']
}


obj = s3.get_object(
    Bucket=tag_dir['bucket'],
    Key=f"{tag_dir['endpoint']}/{tag_dir['file_name']}"
)
tags = json.loads(obj['Body'].read().decode('utf-8'))

url = "https://store.steampowered.com/saleaction/ajaxgetsaledynamicappquery"
http_proxy = random.choice(http_proxies)
params = {
    "cc": "VN",
    "l": "english",
    "start": -123,
    "count": 50,
    "flavor": "contenthub_all",
    "strContentHubType": "tags",
    "nContentHubTagID": tags[0]['tagid'],
    "bContentHubDiscountedOnly": False,
    "bAllowDemos": False,
    "return_capsules": True
}

response = requests.get(
        url = url, 
        proxies = {"http": http_proxy},
        params = params, 
        timeout = 10
)
data = response.json()
print(data)