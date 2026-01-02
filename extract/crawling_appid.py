import boto3
import json
import yaml
import random
import requests
from tqdm import tqdm

# Define constants
HTTP_PROXIES = [line.strip() for line in open(file = "proxies.txt", mode = 'r').readlines()]

# Initialize S3 client
s3 = boto3.client("s3")

# Load S3 configurations
s3_configs = yaml.safe_load(open(file = "configs/aws.yaml", mode = "r").read())

# Define S3 directories
tag_dir = {
    "bucket": s3_configs['s3']['bucket'],
    "endpoint": f"{s3_configs['s3']['raw_prefix']}/{s3_configs['s3']['tag_endpoint']}"
}
app_dir = {
    "bucket": s3_configs['s3']['bucket'],
    "endpoint": f"{s3_configs['s3']['raw_prefix']}/{s3_configs['s3']['appid_endpoint']}"
}

# Fetch tags from S3
obj = s3.get_object(
    Bucket=tag_dir['bucket'],
    Key=f"{tag_dir['endpoint']}/tags.json"
)
tags = json.loads(obj['Body'].read().decode('utf-8'))

# For each tag, fetch associated appIDs and store to S3
for tag in tags:
    # Paginate through results
    last = False
    query = 0
    count = 5000
    
    # Progress bar
    pbar = tqdm(desc=f"Tag {tag['tagid']}, Query", unit=" ")
    
    # While not last page
    while not last:
        
        # Define request parameters
        url = "https://store.steampowered.com/saleaction/ajaxgetsaledynamicappquery"
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
        http_proxy = random.choice(HTTP_PROXIES)
        response = requests.get(
            url = url, 
            proxies = {"http": http_proxy},
            params = params, 
            timeout = 10
        )
        
        # Process response
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