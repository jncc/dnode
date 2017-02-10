import luigi
import docker
import datetime
import os
import json
import boto3
import base64
import logging
from luigi.util import requires

#FILE_ROOT = 's3://jncc-data/workflows/s2ard/'
FILE_ROOT = '/tmp/s2ard'
DOCKER_IMAGE = '914910572686.dkr.ecr.eu-west-1.amazonaws.com/process-test:latest'

logger = logging.getLogger('luigi-interface')

#Create job spec
class CreateJobSpec(luigi.Task):
    text = luigi.Parameter()
    runDate = luigi.DateParameter(default=datetime.datetime.now())

    def run(self):
        with self.output().open('w') as jobspec:
           job = {"outputText" : self.text}
           jobspec.write(json.dumps(job))

    def output(self):
        filePath = os.path.join(os.path.join(FILE_ROOT, self.runDate.strftime("%Y-%m-%d")), 'job.json') 

        return luigi.LocalTarget(filePath)

#Process using docker isinstance
@requires(CreateJobSpec)
class CreateArdProduct(luigi.Task):
    
    def run(self):

        #Get docker image repo login
        awsClient = boto3.client('ecr')
        authResponse = awsClient.get_authorization_token()
        token64 = authResponse['authorizationData'][0]['authorizationToken']
        token = base64.b64decode(token64)
        auth = token.split(':')
        logger.info('Got AWS Auth token')

        registry = 'https://' + DOCKER_IMAGE.split('/')[0]

        client = docker.from_env()
        client.login(username=auth[0],password=auth[1],registry=registry)
        logger.info('Pulling docker image %s', DOCKER_IMAGE)
        client.images.pull(DOCKER_IMAGE)

        filePath = os.path.join(FILE_ROOT, self.runDate.strftime("%Y-%m-%d"))

        volumes = {
            filePath : {'bind': '/mnt/state', 'mode': 'rw'}
        }

        environment = {
            "USERID" : os.getuid(),
            "GROUPID" : os.getgid()
        }

        logger.info('Running docker image %s', DOCKER_IMAGE)
        client.containers.run(DOCKER_IMAGE, environment=environment, volumes=volumes,  detach=False)
        
    def output(self):
        filePath = os.path.join(os.path.join(FILE_ROOT, self.runDate.strftime("%Y-%m-%d")), 'output.txt')

        return luigi.LocalTarget(filePath)

#Move result to S3 and catalog

if __name__ == '__main__':
    luigi.run()