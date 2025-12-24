import requests
import random
import boto3
import json
import yaml

http_proxies = [line.strip() for line in open(file = "proxies.txt", mode = 'r').readlines()]

s3 = boto3.client("s3")
s3_configs = yaml.safe_load(open(file = "configs/aws.yaml", mode = "r").read())
tag_dir = {
    "bucket": s3_configs['s3']['bucket'],
    "endpoint": f"{s3_configs['s3']['raw_prefix']}/{s3_configs['s3']['tag_endpoint']}",
    "file_name": s3_configs['s3']['tag_filename']
}

url = "https://store.steampowered.com/actions/ajaxgetstoretags"
http_proxy = random.choice(http_proxies)
response = requests.get(
    url = url,
    proxies={"http": http_proxy}, timeout=10
)
tags = response.json()['tags']


s3.put_object(
    Bucket=tag_dir['bucket'],
    Key=f"{tag_dir['endpoint']}/{tag_dir['file_name']}",
    Body=json.dumps(tags, ensure_ascii=False),
    ContentType="application/json"
)
