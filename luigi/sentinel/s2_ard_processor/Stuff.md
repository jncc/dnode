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

Build image:

    docker build -t process-test .

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

