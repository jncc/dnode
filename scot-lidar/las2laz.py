import argparse
import boto3
import hashlib
import logging
import os
import shutil
import subprocess
import time


def md5(fname):
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def create_laz_product(laszip_path, input_path, output_path):
    p = subprocess.Popen('%s -i %s -o %s' % (laszip_path, input_path, output_path), shell=True)
    (output, err) = p.communicate()
    if output is not None:
        self.logger.debug(output)
    if err is not None:
        raise RuntimeError(err)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Runs through S3 directory looking for las files and compresses them to laz files uploading results to give directory')
    parser.add_argument('-b', '--bucket', type=str, required=True, help='S3 Bucket to look in')
    parser.add_argument('-p', '--profile', type=str, required=True, help='Profile to use when connecting to S3')
    parser.add_argument('-i', '--input', type=str, required=False, default='', help='Folder prefix to use when scanning S3 bucket')
    parser.add_argument('-o', '--output', type=str, required=True, help='Folder prefix to use when outping resultant data to S3 bucket')
    parser.add_argument('-e', '--executable', type=str, required=True, help='Path to executable laszip')
    parser.add_argument('-t', '--tempdir', type=str, required=False, default='./temp', help='Local temporary directory [Default: ./temp]')

    args = parser.parse_args()

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    logger = logging.getLogger('s3_ard_walk_init')
    logger.setLevel(logging.DEBUG)

    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    ch.setFormatter(formatter)

    fh = logging.FileHandler('las2laz-%s.log' % time.strftime('%y%m%d-%H%M%S'))
    fh.setFormatter(formatter)
    fh.setLevel(logging.DEBUG)

    logger.addHandler(ch)
    logger.addHandler(fh)    

    session = boto3.Session(profile_name=args.profile)
    s3 = session.resource('s3')
    s3c = session.client('s3')
    bucket = s3.Bucket(args.bucket)    

    logger.info('Scanning bucket (%s) path \'%s\' for .las files' % (args.bucket, args.input))
    for match in bucket.objects.filter(Prefix=args.input):
        if match.key.endswith('.las') or match.key.endswith('.LAS'):
            try:
                logger.info('Downloading .las file - %s' % match.key)
                input_file = os.path.join(args.tempdir, os.path.basename(match.key))
                output_file = '%s.laz' % (input_file[:-4])
                bucket.download_file(match.key, input_file)

                logger.info('Creating .laz file')
                create_laz_product(args.executable, input_file, output_file)

                logger.info('Calculating checksum for resulting .laz file')
                checksum = md5(output_file)

                logger.info('Uploading resulting .laz file to output path')
                s3c.upload_file(output_file, args.bucket, os.path.join(args.output, os.path.basename(output_file)), ExtraArgs={"Metadata": {"md5": checksum}})

                logger.info('Cleaning up temp files')
                os.unlink(input_file)
                os.unlink(output_file)
            except:
                logger.error('Process errored on %s' % (match.key))
                logger.error('Error is:')
                logger.error(sys.exc_info()[0].message)
    logger.info('Finished scanning bucket with prefix \'%s\'' % args.input)
