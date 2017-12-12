

# Gets the current list of objects (key and size) in the S3 bucket.
# You need read access to S3, so you can use a user such as the "s3-read-only" user.
# To create a local security profile called 's3-read-only', run
# `aws configure --profile s3-read-only`
# add the key details, and then
# `python list.py --profile s3-read-only`.


# example:
# python get.py --profile s3-read-only

import argparse
import boto3
import os
from datetime import datetime, timezone


def main():
    args = parse_command_line_args()

    print('Starting...')
    session = boto3.Session(profile_name=args.profile)
    bucket = session.resource('s3').Bucket(args.bucket)
    # s3c = session.client('s3')

    filename = 'list-' + datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S") + '.txt'    

    print('Scanning %s/%s...' % (args.bucket, args.path))
    for o in bucket.objects.filter(Prefix=args.path).limit(args.limit):
        with open(os.path.join('.', args.outdir, filename), 'a') as f:
            f.write('%s %s\n' % (o.key, o.size))

def parse_command_line_args():
    parser = argparse.ArgumentParser(
        description='Runs through S3 directory looking for S2 ARD images and saves the filename to an output file.')
    parser.add_argument('-p', '--profile', type=str, required=True, help='Profile to use when connecting to S3')
    parser.add_argument('-b', '--bucket', type=str, required=False, default='eocoe-sentinel-2', help='S3 bucket to look in')
    parser.add_argument('-l', '--limit', type=int, required=False, default=1000000000, help='Limit the number of S3 objects scanned for dev')
    parser.add_argument('-a', '--path', type=str, required=False, default='initial', help='Folder within S3 bucket')
    parser.add_argument('-o', '--outdir', type=str, required=False, default='output', help='Local output directory [Default: ./output]')
    return parser.parse_args()

main()