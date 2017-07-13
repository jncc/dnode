# Docker host setup
## Configure  to run as non root user

    sudo groupadd docker  
    sudo gpasswd -a ${USER} docker 
    sudo service docker restart 

log back in to apply

# Docker image
- Runs python script processor.py
- Takes in job.json
    {"outputText":"This is some text"}
- Outputs text file output.txt containing value of output text
- Upload image to amazon ECR repo - http://docs.aws.amazon.com/AmazonECR/latest/userguide/docker-push-ecr-image.html

# AWS ECR repo

Repo name:

914910572686.dkr.ecr.eu-west-1.amazonaws.com/process-test

To pull the latest image fom the repo:

    /home/felix/Development/dnode/luigi/sentinel/s2_ard_processor/docker_image/pull.sh

To build and push to the repo:

    ./dnode/luigi/sentinel/s2_ard_processor/docker_image/build.sh

# Build and run instructions

Build to image: 

    docker build -t 914910572686.dkr.ecr.eu-west-1.amazonaws.com/process-test .

## Run time paramters
Parmeters are passed in as environment variables.
- AWSKEY= aws access key
- AWSSECRET= aws secret key 
- S3SOURCEPATH= s3 path to source file including file name 
- S3DESTPATH= s3 path for output file inculding file name

Start image with interactive console: 
 shares the workfiles folder in home
 sets user id and group id

    docker run -i -t -v ~/workfiles:/mnt/state -e AWSKEY= aws access key> -e AWSSECRET=secret -e S3SOURCEPATH=source path S3DESTPATH=output path 914910572686.dkr.ecr.eu-west-1.amazonaws.com/process-test:latest /bin/bash

Just run it:

    docker run -v ~/workfiles:/mnt/state -e AWSKEY= aws access key> -e AWSSECRET=secret -e S3SOURCEPATH=source path S3DESTPATH=output 914910572686.dkr.ecr.eu-west-1.amazonaws.com/process-test:latest
