
Product metadata for Scottish Lidar portal
==========================================


- Ensure Python is installed (I installed the latest Python 3)
- Do all the pip installs listed in the script comments
    pip install boto3
    pip install awscli
- Ensure AWS credentials are configured. you'll need the key id and secret.
    aws configure
- You should be able to run the sanity-check script to enumerate the S3 buckets
    python sanity.py

