import json
import os
import boto3
import yaml
from multiprocessing import Pool
from tqdm import tqdm


# Initialize S3 client
s3 = boto3.client("s3")

# Load S3 configurations
s3_configs = yaml.safe_load(open(file = "configs/aws.yaml", mode = "r").read())

# # Define S3 directories
content_dir = {
    "bucket": s3_configs['s3']['bucket'],
    "endpoint": f"{s3_configs['s3']['raw_prefix']}/{s3_configs['s3']['content_endpoint']}"
}
processed_app_dir = {
    "bucket": s3_configs['s3']['bucket'],
    "endpoint": f"{s3_configs['s3']['processed_prefix']}/{s3_configs['s3']['appid_endpoint']}"
}

# Local bucket folder
FOLDER = "databucket"

def json_to_str(fileName: str) -> str:
    with open(fileName, "r", encoding="utf-8") as f:
        content = json.load(f)
    return json.dumps(content)

doneIdList =  [f.replace(".json", "") for f in os.listdir(FOLDER) if f.endswith(".json")]
fileList = [f"{FOLDER}/{f}" for f in os.listdir(FOLDER) if f.endswith(".json")]

# Instead open file within the function, should've open along with the Pool for optimization:
with Pool(processes=16) as pool, open("app_content.txt", "a", encoding="utf-8") as saveFile:
    for result in tqdm(pool.imap_unordered(json_to_str, fileList), total=len(fileList)):
        saveFile.write(result + "\n")

# Save done id list to a .txt file
fileName = "done_appids.txt"
with open(fileName, "w", encoding="utf-8") as f:
    for appid in doneIdList:
        f.write(appid + "\n")


# Upload content file to S3
s3.upload_file(
    Filename="app_content.txt",
    Bucket=content_dir['bucket'],
    Key=f"{content_dir['endpoint']}/app_content.txt"
)

# Upload id file to S3
s3.upload_file(
    Filename="done_appids.txt",
    Bucket=processed_app_dir['bucket'],
    Key=f"{processed_app_dir['endpoint']}/done_appids.txt"
)