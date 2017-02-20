DL_LOGIN=$(aws ecr get-login)
eval $DL_LOGIN
docker pull 914910572686.dkr.ecr.eu-west-1.amazonaws.com/process-test:latest
