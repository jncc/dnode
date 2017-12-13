
import argparse
import boto3
import json
import os

# script to generate a thumbnail
import thumbnail

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
    s3_key = 'initial/UKSentinel2A_20150628/SEN2_20150628_lat52lon366_T30UVC_ORB077_utm30n_osgb_vmsk_sharp_rad_srefdem_stdsref.tif'
    product_file_name = os.path.basename(s3_key)
    product_path = os.path.join('.', product_file_name)
    s3_client.download_file(s3_bucket, s3_key, product_path)

    exit()

    # todo open success file to append to

    count = 0
    for p in products.keys():
        count += 1
        name = p
        attrs = products[p]['attrs']
        files = products[p]['files']
        product_file = next((f for f in files if f['type']=='product'), None)
        print(product_file)
        if product_file is not None:
            s3_key = product_file['data']
            # product_file_name = os.path.basename(s3_key)
            # print(product_file_name)
            # full_out_dir = os.path.join('.', outdir, 'thumbnails')
            # local_product_path = os.path.join(full_out_dir, product_file_name)
            # if not os.path.exists(full_out_dir):
            #     os.makedirs(full_out_dir)
            # s3_client.download_file(s3_bucket, s3_key, local_product_path)
            # local_thumbnail_path = os.path.join(full_out_dir, thumbnail_file_name)
            create_thumbnail_x(s3_client, product_file, args.outdir)


# def create_thumbnail_x(s3_client, product_file, outdir):
def create_single_thumbnail(input_path):
    output_path = input_path.replace('_vmsk_sharp_rad_srefdem_stdsref.tif', '_thumbnail.jpg')
    shell_command = 'gdal_translate -b 3 -b 2 -b 1 -ot Byte -of JPEG -outsize 5%% 5%% %s %s' % (input_path, output_path)
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
