import boto3
import yaml

# Initialize S3 client
s3 = boto3.client("s3")

# Load S3 configurations
s3_configs = yaml.safe_load(open(file = "configs/aws.yaml", mode = "r").read())

# Define S3 directories
review_dir = {
    "bucket": s3_configs['s3']['bucket'],
    "endpoint": f"{s3_configs['s3']['raw_prefix']}/{s3_configs['s3']['review_endpoint']}"
}

# Upload content file to S3
fileName = "databucket/app_review.txt"
s3.upload_file(
    Filename=fileName,
    Bucket=review_dir['bucket'],
    Key=f"{review_dir['endpoint']}/app_review.txt"
)
