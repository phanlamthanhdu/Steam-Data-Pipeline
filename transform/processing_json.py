import json
import os



FOLDER = "databucket"
for file in os.listdir(FOLDER):
    if file.endswith(".json"):
        appid = file.replace(".json", "")
        with open(f"{FOLDER}/{file}", "r", encoding='utf-8') as f:
            content = json.load(f)
        
        fileName = "app_content.txt"
        with open(fileName, "a", encoding='utf-8') as f:
            f.write(f"{str(content)}\n")