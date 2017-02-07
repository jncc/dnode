#!/bin/bash
DL_LOGIN=$(aws ecr get-login)
eval $DL_LOGIN
docker build -t process-test .
docker tag process-test:latest 914910572686.dkr.ecr.eu-west-1.amazonaws.com/process-test:latest
docker push 914910572686.dkr.ecr.eu-west-1.amazonaws.com/process-test:latest
