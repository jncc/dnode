"""
Based on a file extension return a vague type of file to be added as metadata in the database catalog

:param ext: The extension of a file you wanted to label
:return: A vague type for that sort of file
"""
def get_file_type(ext):
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
def get_representation(bucket, region, path, file_type):
    return {
        'bucket': bucket,
        'region': region,
        'path': path,
        'url': 'https://s3-%s.amazonaws.com/%s%s' % (bucket, region, path),
        'type': file_type
    }
