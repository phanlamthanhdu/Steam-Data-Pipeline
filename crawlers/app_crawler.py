import boto3
import json
import yaml
import random
import requests
from tqdm import tqdm

http_proxies = [line.strip() for line in open(file = "proxies.txt", mode = 'r').readlines()]

s3 = boto3.client("s3")

s3_configs = yaml.safe_load(open(file = "configs/aws.yaml", mode = "r").read())
tag_dir = {
    "bucket": s3_configs['s3']['bucket'],
    "endpoint": f"{s3_configs['s3']['raw_prefix']}/{s3_configs['s3']['tag_endpoint']}",
}
app_dir = {
    "bucket": s3_configs['s3']['bucket'],
    "endpoint": f"{s3_configs['s3']['raw_prefix']}/{s3_configs['s3']['app_endpoint']}",
}


obj = s3.get_object(
    Bucket=tag_dir['bucket'],
    Key=f"{tag_dir['endpoint']}/tags.json"
)
tags = json.loads(obj['Body'].read().decode('utf-8'))

url = "https://store.steampowered.com/saleaction/ajaxgetsaledynamicappquery"
for tag in tags:

    last = False
    query = 0
    count = 5000
    
    pbar = tqdm(desc=f"Tag {tag['tagid']}, Query", unit=" ")
    
    while not last:
        params = {
            "cc": "VN",
            "l": "english",
            "start": query * count,
            "count": count,
            "flavor": "contenthub_all",
            "strContentHubType": "tags",
            "nContentHubTagID": tag['tagid'],
            "bContentHubDiscountedOnly": False,
            "bAllowDemos": False,
            "return_capsules": True
        }
        http_proxy = random.choice(http_proxies)
        response = requests.get(
            url = url, 
            proxies = {"http": http_proxy},
            params = params, 
            timeout = 10
        )
        if response.status_code == 200:
            tag_apps = response.json()
            
            if len(tag_apps['appids']) == 0:
                last = True
            else:
                s3.put_object(
                    Bucket=app_dir['bucket'],
                    Key=f"{app_dir['endpoint']}/{tag['tagid']}/query_{query}.json",
                    Body=json.dumps(tag_apps["appids"], ensure_ascii=False),
                    ContentType="application/json"
                )
        else:
            print(f"Error {response.status_code} for tag {tag['tagid']} at query {query}")
        query += 1
        pbar.update(1)