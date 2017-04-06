import boto
import sys, os
from boto.s3.key import Key
from urllib.parse import urlsplit

#max size in bytes before uploading in parts. between 1 and 5 GB recommended
MAX_SIZE = 5000000000
#size of parts when uploading in parts
PART_SIZE = 100000000
LOCAL_FILE_NAME = '/mnt/state/output.tiff'

AWS_ACCESS_KEY_ID = os.environ['AWSKEY']
AWS_SECRET_ACCESS_KEY = os.environ['AWSSECRET']
S3_DEST_PATH = os.environ['S3DESTPATH']

x = urlsplit(S3_DEST_PATH)

bucket_name = x.netloc

conn = boto.connect_s3(AWS_ACCESS_KEY_ID,
                AWS_SECRET_ACCESS_KEY)
bucket = conn.get_bucket(bucket_name)

filesize = os.path.getsize(LOCAL_FILE_NAME)

if filesize > MAX_SIZE:
    mp = bucket.initiate_multipart_upload(x.path)
    fp = open(LOCAL_FILE_NAME,'rb')
    fp_num = 0
    while (fp.tell() < filesize):
        fp_num += 1
        mp.upload_part_from_file(fp, fp_num, num_cb=10, size=PART_SIZE)

    mp.complete_upload()

else:
    k = boto.s3.key.Key(bucket)
    k.key = x.path
    k.set_contents_from_filename(LOCAL_FILE_NAME, num_cb=10)
