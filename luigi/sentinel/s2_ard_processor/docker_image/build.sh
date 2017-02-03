#!/bin/bash
DL_LOGIN=$(aws ecr get-login)

docker build -t AKIAJIXWW3NPTFVVAQDQ.dkr.ecr.eu-west-1.amazonaws.com/process-test .
