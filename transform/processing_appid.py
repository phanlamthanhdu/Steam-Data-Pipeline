import boto3
import json
import yaml
import random
import requests
from tqdm import tqdm

# Initialize S3 client
s3 = boto3.client("s3")

# Load S3 configurations
s3_configs = yaml.safe_load(open(file = "configs/aws.yaml", mode = "r").read())

# Define S3 directories
app_dir = {
    "bucket": s3_configs['s3']['bucket'],
    "endpoint": f"{s3_configs['s3']['raw_prefix']}/{s3_configs['s3']['appid_endpoint']}"
}
processed_app_dir = {
    "bucket": s3_configs['s3']['bucket'],
    "endpoint": f"{s3_configs['s3']['processed_prefix']}/{s3_configs['s3']['appid_endpoint']}"
}

# Fetch list of appIDs from S3
appList = s3.list_objects_v2(
    Bucket=app_dir['bucket'],
    Prefix=app_dir['endpoint']
)['Contents']


idList = {}
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
    idList.update([str(id) for id in app_data])
    
print(f"Total appIDs: {len(idList)}")
s3.put_object(
    Bucket=processed_app_dir['bucket'],
    Key=f"{processed_app_dir['endpoint']}/all_appids.json",
    Body=json.dumps(idList).encode('utf-8')
)