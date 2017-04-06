
import boto
from boto.s3.key import Key

# set boto lib debug to critical
logging.getLogger('boto').setLevel(logging.CRITICAL)
bucket_name = settings.BUCKET_NAME
# connect to the bucket
conn = boto.connect_s3(settings.AWS_ACCESS_KEY_ID,
                settings.AWS_SECRET_ACCESS_KEY)
bucket = conn.get_bucket(bucket_name)
# go through each version of the file
key = '%s.png' % id
fn = '/var/www/data/%s.png' % id
# create a key to keep track of our file in the storage 
k = Key(bucket)
k.key = key
k.set_contents_from_filename(fn)
# we need to make it public so it can be accessed publicly
# using a URL like http://s3.amazonaws.com/bucket_name/key
k.make_public()
# remove the file from the web server
os.remove(fn)
