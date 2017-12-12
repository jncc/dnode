
import argparse
import boto3
import json
import os
import subprocess

def parse_command_line_args():
    p = argparse.ArgumentParser()
    p.add_argument('-p', '--profile', type=str, required=True, help='Profile to use when connecting to S3')
    p.add_argument('-i', '--input', type=str, required=True, help='Input from previous step output')
    p.add_argument('-o', '--outdir', type=str, required=False, default='output', help='Local output directory [Default: ./output]')
    return p.parse_args()

def main():
    args = parse_command_line_args()

    with open(args.input) as f:
        products = json.load(f)

    print('Loading %s products' % (len(products)))

    session = boto3.Session(profile_name=args.profile)
    s3_client = session.client('s3')

    for p in products.keys():
        name = p
        attrs = products[p]['attrs']
        files = products[p]['files']
        product_file = next((f for f in files if f['type']=='product'), None)
        print(product_file)
        if product_file is not None:
            create_thumbnail_x(s3_client, product_file, args.outdir)


def create_thumbnail_x(s3_client, product_file, outdir):
    s3_bucket = 'eocoe-sentinel-2'
    s3_key = product_file['data']
    product_file_name = os.path.basename(s3_key)
    print(product_file_name)
    full_out_dir = os.path.join('.', outdir, 'thumbnails')
    local_product_path = os.path.join(full_out_dir, product_file_name)
    if not os.path.exists(full_out_dir):
        os.makedirs(full_out_dir)
    s3_client.download_file(s3_bucket, s3_key, local_product_path)

    thumbnail_file_name = product_file_name.replace('_vmsk_sharp_rad_srefdem_stdsref.tif', '_thumbnail.jpg')
    local_thumbnail_path = os.path.join(full_out_dir, thumbnail_file_name)
    print(local_product_path)
    print(local_thumbnail_path)
    shell_command = 'gdal_translate -b 3 -b 2 -b 1 -ot Byte -of JPEG -outsize 5%% 5%% %s %s' % (local_product_path, local_thumbnail_path)
    p = subprocess.Popen(shell_command, shell=True)
    (output, err) = p.communicate()
    if output is not None:
        self.log.debug(output)
    if err is not None:
        raise RuntimeError(err)

    
    
def create_thumbnail(s3client, bucket, product, outdir):
    s3client.download_file(bucket, product, os.path.join(outdir, os.path.basename(product)))

    p = subprocess.Popen('gdal_translate -b 3 -b 2 -b 1 -ot Byte -of JPEG -outsize 5%% 5%% %s %s' % (os.path.join(outdir, os.path.basename(product)), os.path.join(outdir, os.path.basename(product).replace('_vmsk_sharp_rad_srefdem_stdsref.tif', '_thumbnail.jpg'))), shell=True)
    (output, err) = p.communicate()
    if output is not None:
        self.log.debug(output)
    if err is not None:
        raise RuntimeError(err)
    
    s3client.upload_file(os.path.join(outdir, os.path.basename(product).replace('_vmsk_sharp_rad_srefdem_stdsref.tif', '_thumbnail.jpg')), bucket, product.replace('_vmsk_sharp_rad_srefdem_stdsref.tif', '_thumbnail.jpg'))

main()
