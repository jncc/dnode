
import argparse
import boto3
import json
import os

import thumbnail # import our script to generate a thumbnail

def parse_command_line_args():
    p = argparse.ArgumentParser()
    p.add_argument('-p', '--profile', type=str, required=False, help='Profile to use when connecting to S3')
    p.add_argument('-i', '--input', type=str, required=True, help='Input from previous step''s output')
    return p.parse_args()

def main():
    args = parse_command_line_args()

    with open(args.input) as f:
        products = json.load(f)

    print('Loading %s products' % (len(products)))

    session = boto3.Session(profile_name=args.profile)
    s3_client = session.client('s3')
    s3_bucket = 'eocoe-sentinel-2'

    # todo open success file to append to

    count = 0
    for p in products.keys():
        count += 1
        name = p
        attrs = products[p]['attrs']
        files = products[p]['files']
        product_file = next((f for f in files if f['type']=='product'), None)
        if product_file is not None:
            s3_key = product_file['data']
            print('Processing %d of %d. %s' (count, len(products), s3_key))
            product_file_name = os.path.basename(s3_key)
            product_path = os.path.join('.', product_file_name)
            s3_client.download_file(s3_bucket, s3_key, product_path)
            thumbnail.create_single_thumbnail(product_path)
            # now there should be a thumbnail next to the product file!

main()
