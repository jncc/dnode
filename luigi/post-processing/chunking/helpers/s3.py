import boto
import os
import time

from helpers import verification as verificationHelper

class s3:
    def __init__(self):
        pass
    
    """
    Copy a file up to a S3 from the sourcepath to a given filename (full path name), will
    calculate md5 checksums and upload with the file

    :param access_key: Your AWS access key
    :param secret_access_key: Your AWS secret access key
    :param region: The AWS region that the bucket resides in [eu-west-1]
    :param bucket: The AWS bucket name
    :param sourcepath: The source path of the file to upload
    :param key: The destination path of the file being uploaded
    :param public: If the data is to be a public file or not (applies Public ACL)
    :param metadata: Dict containing any additional metadata to be attached to the upload
    """
    def copy_file_to_s3(self, access_key, secret_access_key, region, bucket, sourcepath, key, public, metadata):
        #max size in bytes before uploading in parts. between 1 and 5 GB recommended
        MAX_SIZE = 5000000000
        #size of parts when uploading in parts
        PART_SIZE = 100000000        

        conn = boto.s3.connect_to_region(region, aws_access_key_id=access_key, aws_secret_access_key=secret_access_key, is_secure=True)
        bucket = conn.get_bucket(bucket)
        
        if metadata is None:
            metadata = {}

        metadata['md5'] = verificationHelper.verification().calculate_checksum(sourcepath)
        metadata['uploaded'] = time.strftime('%Y-%m-%dT%H:%M:%SZ')

        if bucket.get_key(key) != None:
            bucket.delete_key(key)            

        filesize = os.path.getsize(sourcepath)
        if filesize > MAX_SIZE:
            if public:
                mp = bucket.initiate_multipart_upload(key, metadata=metadata, policy='public-read')
            else:
                mp = bucket.initiate_multipart_upload(key, metadata=metadata)  

            fp = open(sourcepath,'rb')
            fp_num = 0
            while (fp.tell() < filesize):
                fp_num += 1
                mp.upload_part_from_file(fp, fp_num, num_cb=10, size=PART_SIZE)

            mp.complete_upload()          
        else:
            k = boto.s3.key.Key(bucket)
            k.key = key

            for mkey in metadata:
                k.set_metadata(mkey, metadata[mkey])

            k.set_contents_from_filename(sourcepath, num_cb=10)

            if public:
                k.set_acl('public-read')

    """
    Based on a file extension return a vague type of file to be added as metadata in the database catalog

    :param ext: The extension of a file you wanted to label
    :return: A vague type for that sort of file
    """
    def get_file_type(self, ext):
        if ext == '.tif':
            return 'data'
        elif ext == '.geojson':
            return 'footprint'
        elif ext == '.cpg' or ext == '.dbf' or ext == '.prj' or ext =='.qpj' or ext == '.shp' or ext == '.shx':
            return 'footprint-shapefile'
        elif ext == '.xml':
            return 'metadata'
        elif ext == '.jpeg' or ext == '.jpg' or ext == '.png':
            return 'preview'
        else:
            return 'unknown'

    """
    Get a representation of a file in S3, with all the relevant data needed to access the file

    :param bucket: The name of the bucket the file is in
    :param region: The region that the bucket is in
    :param path: The path that the file exists on in S3
    :parm file_type: A quick file type description, to try and make it easier to differentiate files
    :return: A JSON representation of the file in S3
    """
    def get_representation(self, bucket, region, path, file_type):
        return {
            'bucket': bucket,
            'region': region,
            'path': path,
            'url': 'https://s3-%s.amazonaws.com/%s%s' % (region, bucket, path),
            'type': file_type
        }
