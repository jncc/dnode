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

    def run(self):
        with self.output().open('w') as jobspec:
           job = {"text" : self.text}
           jobspec.write(json.dumps(job))

    def output(self):
        d = datetime.datetime.now()
        filePath = os.path.join(os.path.join(FILE_ROOT, d.strftime("%Y-%m-%d")), 'job.json')
        print 

        return luigi.LocalTarget(filePath)

#Process using docker isinstance
@requires(CreateJobSpec)
class CreateArdProduct(luigi.Task):
    
    def run(self):
        with self.input().open() as jobspec, self.output().open('w') as output:
            job = json.load(jobspec)
            output.write(job["text"])
        
    def output(self):
        d = datetime.datetime.now()
        filePath = os.path.join(os.path.join(FILE_ROOT, d.strftime("%Y-%m-%d")), 'output.txt')

        return luigi.LocalTarget(filePath)

#Move result to S3 and catalog

if __name__ == '__main__':
    luigi.run()