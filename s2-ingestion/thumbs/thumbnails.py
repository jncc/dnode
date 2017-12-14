
import argparse
import boto3
import json
import logging
import os
import time
import re

import thumbnail # import our script to generate a thumbnail

log = logging.getLogger('log')

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

    log.info('Loading %s products' % (len(products)))

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
            s3_key = product_file['data']
            log.info('Processing %d of %d. %s' % (count, len(products), s3_key))
            product_file_name = os.path.basename(s3_key)
            product_path = os.path.join('.', product_file_name)
            s3_client.download_file(s3_bucket, s3_key, product_path)
            log.info('Downloaded product.')
            thumbnail_path = thumbnail.create_single_thumbnail(product_path)
            log.info('Generated thumbnail.')
            # now there should be a thumbnail next to the product file!
            thumbnail_file_name = os.path.basename(thumbnail_path)
            thumbnail_s3_key = 'thumbnails/' + thumbnail_file_name
            extra_args = {'ACL':'public-read', 'ContentType': 'image/jpeg'}
            s3.Bucket(s3_bucket).upload_file(thumbnail_path, thumbnail_s3_key, ExtraArgs=extra_args)
            log.info('Uploaded thumbnail.')
            remove_files('.', 'SEN2_')
            log.info('Cleaned up.')

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
