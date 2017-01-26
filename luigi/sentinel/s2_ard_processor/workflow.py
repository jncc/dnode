import luigi
import docker
import datetime
import os
import json

from luigi.util import requires

#FILE_ROOT = 's3://jncc-data/workflows/sentinel-download/'
FILE_ROOT = '/home/felix/temp/s2ard'
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
        client.containers.run("process-test", environment=environment, volumes=volumes,  detach=False)
        
    def output(self):
        filePath = os.path.join(os.path.join(FILE_ROOT, self.runDate.strftime("%Y-%m-%d")), 'output.txt')

        return luigi.LocalTarget(filePath)

#Move result to S3 and catalog

if __name__ == '__main__':
    luigi.run()