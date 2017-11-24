

print("Hello world")

import boto3

s3 = boto3.resource('s3')

# print out bucket names
for bucket in s3.buckets.all():
    print(bucket.name)

