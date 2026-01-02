import boto3
import json
import yaml
import random
import requests
from tqdm import tqdm
from multiprocessing import Pool
import time

# Define constants
## Initialize proxies and user agents
HTTP_PROXIES = [line.strip() for line in open(file = "http_proxies.txt", mode = 'r').readlines()]
HTTPS_PROXIES = [line.strip() for line in open(file = "https_proxies.txt", mode = 'r').readlines()]
USER_AGENTS = [line.strip() for line in open(file = "user_agents.txt", mode = 'r').readlines()]
## Define number of worker processes
WORKERS = 4

# Initialize S3 client
s3 = boto3.client("s3")

# Load S3 configurations
s3_configs = yaml.safe_load(open(file = "configs/aws.yaml", mode = "r").read())

# Define S3 directories
processed_app_dir = {
    "bucket": s3_configs['s3']['bucket'],
    "endpoint": f"{s3_configs['s3']['processed_prefix']}/{s3_configs['s3']['appid_endpoint']}"
}
content_dir = {
    "bucket": s3_configs['s3']['bucket'],
    "endpoint": f"{s3_configs['s3']['raw_prefix']}/{s3_configs['s3']['content_endpoint']}"
}

# Fetch list of appIDs from S3
idList_json = s3.get_object(
    Bucket=processed_app_dir['bucket'],
    Key=f"{processed_app_dir['endpoint']}/all_appids.json"
)
idList = json.loads(idList_json['Body'].read())

# Fetch list of already content-crawled appIDs from S3
crawled_idList = set({})
paginator = s3.get_paginator("list_objects_v2")
for page in paginator.paginate(
    Bucket=content_dir['bucket'],
    Prefix=content_dir['endpoint']
):
    if "Contents" in page:
        crawled_idList.update([obj['Key'].split("/")[-1].replace(".json", "")
            for obj in page["Contents"] if obj["Size"] > 10
        ])
        
# Function to get content by appid
def get_content_by_appid(appid: str):
    """Fetch content details for a given Steam AppID and store it in S3."""
    
    try: # Attempt to fetch content from the API
        # Make the API request to fetch app details
        gotContent = False
        while not gotContent:
            # Randomly select a proxy and user-agent
            proxy = random.choice(HTTP_PROXIES)
            
            # Set up request headers
            headers = {
                "User-Agent": random.choice(USER_AGENTS),
                "Accept": "application/json,text/javascript,*/*;q=0.01",
                "Accept-Language": "en-US,en;q=0.9",
                "Referer": "https://store.steampowered.com/",
                "Connection": "keep-alive",
            }
            
            # Introduce a small random delay to avoid rate limiting
            time.sleep(1.5)

            response = requests.get(
                url = f"https://store.steampowered.com/api/appdetails?",
                params = {
                    "appids": appid,
                    "cc": "US"
                },
                headers=headers,
                proxies = {
                    "http": proxy
                },
                timeout=20
            )

            content = response.json()
        
            if len(str(content)) > 10:
                gotContent = True
        s3.put_object(
            Bucket=content_dir['bucket'],
            Key=f"{content_dir['endpoint']}/{appid}.json",
            Body=json.dumps(content)
        )
        return ("Successful", appid)
    except Exception as e: # Handle any exceptions during the request or storage
        print(f"Failed to fetch content for AppID {appid}")
        return ("Failed", appid)


# Fetch content for all uncrawled appIDs using multiprocessing
with Pool(processes=WORKERS) as pool:
    ids = list(
        tqdm(
            desc="Fetching content",
            iterable=pool.imap_unordered(get_content_by_appid, idList),
            total=len(idList)
        )
    )