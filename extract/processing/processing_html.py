import boto3
import json
import yaml
from tqdm import tqdm
from multiprocessing import Pool
from bs4 import BeautifulSoup
import os


# Define generator to yield the HTML content from local databucket line by line
def html_line_generator(folder: str):
    """
    Generator to yield appid and HTML content from files in the specified folder.
    """
    
    # Iterate through files in the folder
    for file in os.listdir(folder):
        if file.endswith(".html"):
            appid = file.replace(".html", "")
            
            # Read the HTML content from the file
            with open(f"{folder}/{file}", "r", encoding='utf-8') as f:
                html_content = f.read()
                yield appid, html_content
                
                # # Remove file after reading
                # os.remove(f"{folder}/{file}")

count = 0
for appid, html_content in html_line_generator("databucket"):
    print(appid, end=' ')


    # Parse the HTML content using BeautifulSoup
    html_parser = BeautifulSoup(html_content, 'html.parser')
    name = html_parser.find('div', {'class': 'apphub_AppName'}).text.strip()
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
        orginal_price = html_parser.find_all('div', {'class': 'game_area_purchase_game_wrapper'})[0].find('div', {'class': 'game_area_purchase_game'}).find('div', {'class': 'discount_original_price'}).text.strip().replace('$', '')
    except:
        try:
            orginal_price = html_parser.find('div', {'class': 'game_area_purchase_game'}).find('div', {'class': 'game_purchase_price'}).text.strip().replace('$', '')
        except:
            orginal_price = None
    if orginal_price and orginal_price.find('Free') != -1:
        orginal_price = "Free"
    
    price_overview = {  
        "currency": "USD",
        "original_price": orginal_price
    }

    # Get game description as HTML for further processing
    about_the_game = "".join([str(div) for div in html_parser.find_all('div', {'class': 'game_area_description'})])

    # Get PC requirements as HTML for further processing
    pc_requirements = str(html_parser.find('div', {'class': 'game_page_autocollapse sys_reqs'}))

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
    release_date = html_parser.find('div', {'class': 'release_date'}).find('div', {'class': 'date'}).text.strip()
    
    # Extract developers
    developers = [
        dev.text.strip()
        for dev in html_parser.find('div', {'class': 'summary column', 'id': 'developers_list'}).find_all('a')
    ]
    
    # Extract publishers
    publishers = [
        publisher.text.strip()
        for publisher in html_parser.find(
            name = 'div', 
            class_ = 'subtitle column', 
            string = "Publisher:"
        ).find_next_sibling("div", class_="summary column").find_all('a')
    ]
    
    # Extract genres
    genres = [
        genre.text.strip()
        for genre in html_parser.find("b", string="Genre:").find_next_sibling("span").find_all('a')
    ]
    
    user_tags = [
        tag.text.strip()
        for tag in html_parser.find("div", {'class': 'glance_tags popular_tags'}).find_all('a')
    ]
    
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