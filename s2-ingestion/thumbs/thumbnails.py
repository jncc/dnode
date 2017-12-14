
import argparse
import boto3
import fcntl
import json
import logging
import os
import time
import re

import thumbnail # import our script to generate a thumbnail

log = logging.getLogger('log')

success_file = './success.txt'

def parse_command_line_args():
    p = argparse.ArgumentParser()
    p.add_argument('-p', '--profile', type=str, required=False, help='Security profile to use for S3')
    p.add_argument('-i', '--input', type=str, required=True, help='Input from previous step''s output')
    return p.parse_args()

def main():
    args = parse_command_line_args()
    initialise_log()

    with open(args.input) as f:
        products = json.load(f)
    log.info('Found %s products' % (len(products)))

    s3 = boto3.resource('s3')
    session = boto3.Session(profile_name=args.profile)
    s3_client = session.client('s3')
    s3_bucket = 'eocoe-sentinel-2'

    count = 0

    for p in products.keys():
        count += 1
        stopwatch = time.time()
        name = p
        attrs = products[p]['attrs']
        files = products[p]['files']
        product_file = next((f for f in files if f['type']=='product'), None)
        if product_file is not None:
            product_s3_key = product_file['data']
            log.info('Processing %d of %d. %s' % (count, len(products), product_s3_key))
            product_file_name = os.path.basename(product_s3_key)
            product_path = os.path.join('.', product_file_name)
            # download product
            s3_client.download_file(s3_bucket, product_s3_key, product_path)
            log.info('Downloaded product.')
            # generate thumbnail
            thumbnail_path = thumbnail.create_single_thumbnail(product_path)
            log.info('Generated thumbnail.')
            # now there should be a thumbnail next to the product file!
            thumbnail_file_name = os.path.basename(thumbnail_path)
            thumbnail_s3_key = 'thumbnails/' + thumbnail_file_name
            # upload thumbnail
            extra_args = {'ACL':'public-read', 'ContentType': 'image/jpeg'}
            s3.Bucket(s3_bucket).upload_file(thumbnail_path, thumbnail_s3_key, ExtraArgs=extra_args)
            log.info('Uploaded thumbnail.')
            # clean up
            remove_files('.', 'SEN2_')
            log.info('Cleaned up.')
            # record success
            record_success(product_s3_key)


# append the product key to the success file using a lock
def record_success(product_s3_key):
    with open(success_file, 'a') as g:
        fcntl.flock(g, fcntl.LOCK_EX)
        g.write(product_s3_key + '\n')
        fcntl.flock(g, fcntl.LOCK_UN)


def remove_files(dir, pattern):
    for f in os.listdir(dir):
        if re.search(pattern, f):
            os.remove(os.path.join(dir, f))

def initialise_log():
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    log.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    ch.setFormatter(formatter)
    log.addHandler(ch)
    log.info('Logger initialised.')

main()
