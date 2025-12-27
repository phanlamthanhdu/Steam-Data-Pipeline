from weakref import proxy
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
WORKERS = 8


# Initialize S3 client
s3 = boto3.client("s3")

# Load S3 configurations
s3_configs = yaml.safe_load(open(file = "configs/aws.yaml", mode = "r").read())

# Define S3 directories
app_dir = {
    "bucket": s3_configs['s3']['bucket'],
    "endpoint": f"{s3_configs['s3']['raw_prefix']}/{s3_configs['s3']['app_endpoint']}"
}
content_dir = {
    "bucket": s3_configs['s3']['bucket'],
    "endpoint": f"{s3_configs['s3']['raw_prefix']}/{s3_configs['s3']['content_endpoint']}"
}
processed_app_dir = {
    "bucket": s3_configs['s3']['bucket'],
    "endpoint": f"{s3_configs['s3']['processed_prefix']}/{s3_configs['s3']['app_endpoint']}"
}

# Fetch list of appIDs from S3
appList = s3.list_objects_v2(
    Bucket=app_dir['bucket'],
    Prefix=app_dir['endpoint']
)['Contents']

# Fetch list of already content-crawled appIDs from S3
crawled_idList = set({})
paginator = s3.get_paginator("list_objects_v2")
for page in paginator.paginate(
    Bucket=content_dir['bucket'],
    Prefix=content_dir['endpoint']
):
    if "Contents" in page:
        crawled_idList.update([obj['Key'].split("/")[-1].replace(".json", "") 
            for obj in page["Contents"]
        ])

# Function to get content by appid
def get_content_by_appid(appid: int):
    """Fetch content details for a given Steam AppID and store it in S3."""
    
    # Convert appid to string
    id = str(appid)
    
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
    time.sleep(random.uniform(0.0, WORKERS * 0.3))

    # Make the API request to fetch app details
    response = requests.get(
        url = f"https://store.steampowered.com/api/appdetails?appids={id}&cc=US",
        headers=headers,
        proxies = {"http": proxy},
        timeout=20
    )
    
    try: # Try to parse and store the content
        content = response.json()
        s3.put_object(
            Bucket=content_dir['bucket'],
            Key=f"{content_dir['endpoint']}/{id}.json",
            Body=json.dumps(content)
        )
        return ("Successful", id)
    except Exception as e: # Handle any exceptions during the request or storage
        print(f"Failed to fetch content for AppID {id}")
        return ("Failed", id)

for tagDir in tqdm(desc="Processing tag list", iterable=appList):
    # Extract tag and corresponding file directory
    fileDir = tagDir['Key']
    tag = fileDir.split("/")[-2]
    
    # Fetch appIDs from S3
    s3_object = s3.get_object(
        Bucket=app_dir['bucket'],
        Key=fileDir
    )
    app_data = json.loads(s3_object['Body'].read())

    # Skip already crawled appIDs (Resumable crawling)
    i = 0
    while i < len(app_data) and str(app_data[i]) in crawled_idList:
        i += 1
    app_data = app_data[i:]

    # Multiprocessing pool to fetch content concurrently
    with Pool(processes=WORKERS) as pool:
        ids = list(
            tqdm(
                desc=f"Fetching content for tag: {tag} ({i}/{i + len(app_data)} Done)",
                iterable=pool.imap_unordered(get_content_by_appid, app_data),
                total=len(app_data),
                leave=False
            )
        )
    
    