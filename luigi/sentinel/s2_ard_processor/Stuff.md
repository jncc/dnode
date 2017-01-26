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

914910572686.dkr.ecr.eu-west-1.amazonaws.com/process-test

To install the AWS CLI and Docker and for more information on the steps below, visit the ECR documentation page.
1) Retrieve the docker login command that you can use to authenticate your Docker client to your registry:
aws ecr get-login --region eu-west-1

2) Run the docker login command that was returned in the previous step.
3) Build your Docker image using the following command. For information on building a Docker file from scratch see the instructions here. You can skip this step if your image is already built:
docker build -t process-test .

4) After the build completes, tag your image so you can push the image to this repository:
docker tag process-test:latest 914910572686.dkr.ecr.eu-west-1.amazonaws.com/process-test:latest

5) Run the following command to push this image to your newly created AWS repository:
docker push 914910572686.dkr.ecr.eu-west-1.amazonaws.com/process-test:latest

# Build and run instructions

Build image:

    docker build -t process-test .

Build to aws: 

    docker build -t 914910572686.dkr.ecr.eu-west-1.amazonaws.com/process-test .

Start image with interactive console: 
 shares the workfiles folder in home
 sets user id and group id

    docker run -i -t -v ~/workfiles:/mnt/state -e USERID=$UID -e GROUPID=$GID process-test /bin/bash

Just run it:

    docker run -v ~/workfiles:/mnt/state -e USERID=$UID -e GROUPID=$GID process-test


# Create workflow
## GenerateJob
- Creates job.json

Outputs: job.json
## ProcessJob
Requires - GenerateJob
- Instatiates docker image
- copies job.json to image
- Triggers proecess
- copies result back
Outputs - output.txt
### Nice to haves 
- Stream logs (stdout, stderr) to local log file

#run test job
    source ../../luigi_venv/bin/activate
    PYTHONPATH='.' luigi --module workflow CreateArdProduct --local-scheduler --text "this is a test" --runDate=2017-01-26

