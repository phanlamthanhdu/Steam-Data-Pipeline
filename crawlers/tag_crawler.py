import requests
import random
import boto3
import json
import yaml

# Define constants
HTTP_PROXIES = [line.strip() for line in open(file = "proxies.txt", mode = 'r').readlines()]

# Initialize S3 client
s3 = boto3.client("s3")

# Load S3 configurations
s3_configs = yaml.safe_load(open(file = "configs/aws.yaml", mode = "r").read())

# Define S3 directories
tag_dir = {
    "bucket": s3_configs['s3']['bucket'],
    "endpoint": f"{s3_configs['s3']['raw_prefix']}/{s3_configs['s3']['tag_endpoint']}",
}

# Fetch tags from Steam API
url = "https://store.steampowered.com/actions/ajaxgetstoretags"
http_proxy = random.choice(HTTP_PROXIES)
response = requests.get(
    url = url,
    proxies={"http": http_proxy}, timeout=10
)
tags = response.json()['tags']

# Store tags to S3
s3.put_object(
    Bucket=tag_dir['bucket'],
    Key=f"{tag_dir['endpoint']}/tags.json",
    Body=json.dumps(tags, ensure_ascii=False),
    ContentType="application/json"
)
