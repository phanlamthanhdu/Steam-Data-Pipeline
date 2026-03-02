import boto3
import json
import yaml
import random
import requests
from tqdm import tqdm
from multiprocessing import Pool
import time
import os
from bs4 import BeautifulSoup
from urllib.parse import quote
from langdetect import detect, DetectorFactory
DetectorFactory.seed = 0


# Define constants
## Initialize proxies and user agents
HTTP_PROXIES = [line.strip() for line in open(file = "http_proxies.txt", mode = 'r').readlines()]
HTTPS_PROXIES = [line.strip() for line in open(file = "https_proxies.txt", mode = 'r').readlines()]
USER_AGENTS = [line.strip() for line in open(file = "user_agents.txt", mode = 'r').readlines()]

## Define number of worker processes
WORKERS = 16

# Initialize S3 client
s3 = boto3.client("s3")

# Load S3 configurations
s3_configs = yaml.safe_load(open(file = "configs/aws.yaml", mode = "r").read())

# Define S3 directories
review_dir = {
    "bucket": s3_configs['s3']['bucket'],
    "endpoint": f"{s3_configs['s3']['processed_prefix']}/{s3_configs['s3']['review_endpoint']}"
}
processed_app_dir = {
    "bucket": s3_configs['s3']['bucket'],
    "endpoint": f"{s3_configs['s3']['processed_prefix']}/{s3_configs['s3']['appid_endpoint']}"
}

# Fetch list of appIDs from S3
doneIdList = s3.get_object(
    Bucket=processed_app_dir['bucket'],
    Key=f"{processed_app_dir['endpoint']}/done_appids.txt"
)

# .decode("utf-8") to decode binary string to normal string 
doneIdList = doneIdList['Body'].read().decode("utf-8").splitlines()

# Read resumable file
resumableFileName = "resumable/resumable_app_review.txt"
try:
    with open(resumableFileName, "r", encoding="utf-8") as readFile:
        crawledIdList = {line.strip() for line in readFile.readlines()}
except:
    crawledIdList = []


# Filter out already crawled appIDs
IdList = [appid for appid in doneIdList if appid not in crawledIdList]

def is_english(text: str) -> bool:
    try:
        return detect(text) == "en"
    except:
        return False

def get_reviews_by_appid(appid: str):
    """
    Fetch review details for a given Steam AppID and store it in S3.
    """

    try: # Attempt to fetch content from the website
        cursor = "*"
        reviewList = []
        while True: # Infinite loop break with ifs
            
            response = None
            
            # Try to request 5 times until completely getting the response, unless go to Except clause
            for i in range(5):
                # Randomly select a proxy and user-agent
                proxy = random.choice(HTTPS_PROXIES)
                
                # Set up request headers
                headers = {
                    "User-Agent": random.choice(USER_AGENTS),
                    "Accept": "application/json,text/javascript,*/*;q=0.01",
                    "Accept-Language": "en-US,en;q=0.9",
                    "Referer": "https://store.steampowered.com/",
                    "Connection": "keep-alive",
                }
                
                # Set up base url for crawling
                url = f"https://store.steampowered.com/appreviews/{appid}?json=1&num_per_page=100&filter=recent&language=english&review_type=all&cursor={quote(cursor)}"
                
                # Get response from request get the url
                response = requests.get(
                    url = url,
                    headers=headers,
                    proxies = {
                        "http": proxy,
                    },
                    timeout=20
                )
                
                # If getting response, break
                if not response:
                    break
        
            content = response.json()
            reviews = content["reviews"]
            next_cursor = content["cursor"]
            
            # Check whether English (langdetect), len(str) > 10
            reviewList.extend([rev for rev in reviews 
                               if len(rev["review"]) > 10 
                               and is_english(rev["review"])])
            
            # No review returns
            if len(reviews) == 0:
                break
            # Enough reviews
            if len(reviewList) >= 100:
                break
            # Cursor not changed
            if next_cursor == cursor:
                break
            
            cursor = next_cursor
        return {
            "appid": appid,
            "reviews": reviewList
        }
    except Exception as e:
        print(f"Failed to fetch content for AppID {appid}: {e} at line {e.__traceback__.tb_lineno}")
        return None

saveFileName = "databucket/app_review.txt"
WORKERS = 16
with Pool(processes=WORKERS) as pool, \
    open(saveFileName, "a", encoding="utf-8") as saveFile, \
    open(resumableFileName, "a", encoding="utf-8") as resumableFile:
    for result in tqdm(
            desc=f"Fetching reviews (Done: {len(crawledIdList)}/{len(doneIdList)})",
            iterable=pool.imap_unordered(get_reviews_by_appid, IdList),
            total=len(IdList)
    ):
        if result != None:
            saveFile.write(json.dumps(result) + "\n")
            resumableFile.write(result["appid"] + "\n")
            
            saveFile.flush()
            resumableFile.flush()

