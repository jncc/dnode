import luigi
import docker
import datetime
import os
import json

from luigi.util import requires

#FILE_ROOT = 's3://jncc-data/workflows/s2ard/'
FILE_ROOT = '/home/felix/temp/s2ard'
DOCKER_IMAGE = '914910572686.dkr.ecr.eu-west-1.amazonaws.com/process-test'

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
        filePath = os.path.join(FILE_ROOT, self.runDate.strftime("%Y-%m-%d"))

        volumes = {
            filePath : {'bind': '/mnt/state', 'mode': 'rw'}
        }

        environment = {
            "USERID" : os.getuid(),
            "GROUPID" : os.getgid()
        }

        client = docker.from_env()
        client.containers.run(DOCKER_IMAGE, environment=environment, volumes=volumes,  detach=False)
        
    def output(self):
        filePath = os.path.join(os.path.join(FILE_ROOT, self.runDate.strftime("%Y-%m-%d")), 'output.txt')

        return luigi.LocalTarget(filePath)

#Move result to S3 and catalog

if __name__ == '__main__':
    luigi.run()