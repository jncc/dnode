import boto
import sys, os
import logging
from boto.s3.key import Key
from urllib.parse import urlsplit

LOCAL_FILE_NAME = '/mnt/state/input.zip'

# environment variables
AWS_ACCESS_KEY_ID = os.environ['AWSKEY']
AWS_SECRET_ACCESS_KEY = os.environ['AWSSECRET']
SOURCE_PATH = os.environ['S3SOURCEPATH']

x = urlsplit(SOURCE_PATH)
bucket_name = x.netloc

logging.info("established connection to bucket: " + bucket_name)

conn = boto.connect_s3(AWS_ACCESS_KEY_ID,
                AWS_SECRET_ACCESS_KEY)
bucket = conn.get_bucket(bucket_name)

key = bucket.get_key(x.path)

logging.info("downloading " + x.path)
key.get_contents_to_filename(LOCAL_FILE_NAME)
