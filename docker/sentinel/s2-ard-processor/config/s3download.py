import boto
import sys, os
from boto.s3.key import Key
from urllib.parse import urlsplit

LOCAL_PATH = '/mnt/state'
AWS_ACCESS_KEY_ID = os.environ['AWSKEY']
AWS_SECRET_ACCESS_KEY = os.environ['AWSSECRET']

x = urlsplit(os.environ['S3SOURCEPATH'])
bucket_name = x.netloc

conn = boto.connect_s3(AWS_ACCESS_KEY_ID,
                AWS_SECRET_ACCESS_KEY)
bucket = conn.get_bucket(bucket_name)

filename = os.path.split(x.path)[1]

key = bucket.get_key(x.path)
localTarget = os.path.join(LOCAL_PATH, filename)
key.get_contents_to_filename(localTarget)
