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

# Fetch list of already content-crawled appIDs from local databucket
crawled_idList = set({})
for file in os.listdir("databucket"):
    if file.endswith(".json"):
        appid = file.replace(".json", "")
        crawled_idList.add(appid)

ID_num = len(set(idList)) # Havent remove duplicate


# Filter out already crawled appIDs
idList = [appid for appid in idList if appid not in crawled_idList]


def html_content_processing(html_content: str, appid: str) -> dict:
    """
    Process the HTML content to extract relevant details.
    """

    # Parse the HTML content using BeautifulSoup
    html_parser = BeautifulSoup(html_content, 'html.parser')
    
    # Parse the HTML content using BeautifulSoup
    html_parser = BeautifulSoup(html_content, 'html.parser')
    try:
        name = html_parser.find('div', {'class': 'apphub_AppName'}).text.strip()
    except:
        return {}
    
    game_wraper = html_parser.find('div', {"class": "game_area_purchase"})
    app_type = "game"
    if game_wraper.find('div', {'class': 'game_area_dlc_bubble'}):
        app_type = "dlc"
    if game_wraper.find('div', {'class': 'game_area_soundtrack_bubble'}):
        app_type = "soundtrack"
    age_tab = html_parser.find('div', {'class': 'game_rating_icon'})
    required_age = "0"
    if age_tab:
        url = age_tab.find('img')['src']
        required_age = (url.split('/')[-1].split(".png")[0])

    dlcList = [] if app_type == "game" else None
    dlcSection = html_parser.find('div', {'class': 'game_area_dlc_section'})
    if dlcSection:
        dlcTags = dlcSection.find_all('a', {'class': 'game_area_dlc_row'})
        for aTag in dlcTags:
            appID = aTag.get("id").replace("dlc_row_", "")
            dlcList.append(appID)
    
    try:
        orginal_price = [div
                        for div in html_parser.find_all('div', class_="game_area_purchase_game") 
                        if div['class'] == ['game_area_purchase_game'] 
                        and div.find('div', {'class': 'discount_original_price'})
        ][0].find('div', {'class': 'discount_original_price'}).text.strip().replace('$', '')
    except:
        try:
            orginal_price = [div 
                            for div in html_parser.find_all('div', class_="game_area_purchase_game") 
                            if div['class'] == ['game_area_purchase_game']
                            and div.find('div', {'class': 'game_purchase_price'})
            ][0].find('div', {'class': 'game_purchase_price'}).text.strip().replace('$', '')
        except:
            orginal_price = None
            
    if orginal_price and orginal_price.find('Free') != -1:
        orginal_price = "Free"
    
    price_overview = {  
        "currency": "USD",
        "original_price": orginal_price
    }

    # Get game description as HTML for further processing
    try:
        about_the_game = "".join([str(div) for div in html_parser.find_all('div', {'class': 'game_area_description'})])
    except:
        about_the_game = None

    # Get PC requirements as HTML for further processing
    try:
        pc_requirements = str(html_parser.find('div', {'class': 'game_page_autocollapse sys_reqs'}))
    except:
        pc_requirements = None
        
    # user_review, release_date, developers, publishers, genres, user_tags
    try: 
        user_review = {
            metaTag['itemprop']: metaTag['content']
            for metaTag in html_parser.find('a', {'class': 'user_reviews_summary_row', 'itemprop': 'aggregateRating'}).find_all("meta")
        }
        user_review["status"] = html_parser.find('span', {'class': 'game_review_summary','itemprop': 'description'}).text.strip()
    except:
        # Unreleased or no reviews games
        user_review = None
    
    # Extract release date
    try:
        release_date = html_parser.find('div', {'class': 'release_date'}).find('div', {'class': 'date'}).text.strip()
    except:
        release_date = None
    
    # Extract developers
    try:
        developers = [
            dev.text.strip()
            for dev in html_parser.find('div', {'class': 'summary column', 'id': 'developers_list'}).find_all('a')
        ]
    except:
        developers = None
    
    # Extract publishers
    try:
        publishers = [
            publisher.text.strip()
            for publisher in html_parser.find(
                name = 'div', 
                class_ = 'subtitle column', 
                string = "Publisher:"
            ).find_next_sibling("div", class_="summary column").find_all('a')
        ]
    except:
        publishers = None
    
    # Extract genres
    try: 
        genres = [
            genre.text.strip()
            for genre in html_parser.find("b", string="Genre:").find_next_sibling("span").find_all('a')
        ] if app_type != "soundtrack" else None
    except Exception as e:
        genres = None
        

    try:
        user_tags = [
            tag.text.strip()
            for tag in html_parser.find("div", {'class': 'glance_tags popular_tags'}).find_all('a')
        ]
    except:
        user_tags = None

    content_json = {
        "appid": appid,
        "name": name,
        "type": app_type,
        "required_age": required_age,
        "dlc_list": dlcList,
        "price_overview": price_overview,
        "about_the_game": about_the_game,
        "pc_requirements": pc_requirements,
        "user_review": user_review,
        "release_date": release_date,
        "developers": developers,
        "publishers": publishers,
        "genres": genres,
        "user_tags": user_tags
    }
    return content_json
    

# Function to get content by appid
def get_content_by_appid(appid: str):
    """
    Fetch content details for a given Steam AppID and store it in S3.
    """

    try: # Attempt to fetch content from the website
        
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
        
        url = f"https://store.steampowered.com/app/{appid}/?cc=US"
        response = requests.get(
            url = url,
            # headers=headers,
            proxies = {
                "http": proxy,
            },
            timeout=20
        )

        # Get HTML content
        html_content = response.text
        
        # Process HTML content
        content_json = html_content_processing(html_content, appid)
        
        if content_json == {}:
            return ("Failed", appid)
        
        return ("Done", appid, json.dump(content_json))
    except Exception as e: # Handle any exceptions during the request or storage
        # Print error and line of failure
        print(f"Failed to fetch content for AppID {appid}: {e} at line {e.__traceback__.tb_lineno}")
        return ("Failed", appid)

# Done ID List
doneIdList = []

# Save the content to .txt file
fileName = "app_content.txt"

# Fetch content for all uncrawled appIDs using multiprocessing
with Pool(processes=WORKERS) as pool, open(fileName, "a", encoding="utf-8") as saveFile:
    for result in tqdm(
            desc=f"Fetching content (Done: {len(crawled_idList)}/{ID_num})",
            iterable=pool.imap_unordered(get_content_by_appid, idList),
            total=len(idList)
    ):
        if result[0] == "Done":
            doneIdList.append(result[1])
            saveFile.write(result[2] + "\n")

# Save done id list to a .txt file
fileName = "done_appids.txt"
with open(fileName, "w", encoding="utf-8") as f:
    for appid in doneIdList:
        f.write(appid + "\n")
        
# Upload content file to S3
s3.upload_file(
    Filename="app_content.txt",
    Bucket=content_dir['bucket'],
    Key=content_dir['endpoint']
)

# Upload id file to S3
s3.upload_file(
    Filename="done_appids.txt",
    Bucket=processed_app_dir['bucket'],
    Key=f"{processed_app_dir['endpoint']}/done_appids.txt"
)