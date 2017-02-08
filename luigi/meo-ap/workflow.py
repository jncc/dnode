import luigi
import docker
import datetime
import os
import json

from ftp_client import FTPClient
from config_manager import ConfigManager
from luigi.util import requires

#FILE_ROOT = 's3://jncc-data/workflows/s2ard/'
FILE_ROOT = '/tmp/meo-ap'
#DOCKER_IMAGE = '914910572686.dkr.ecr.eu-west-1.amazonaws.com/process-test'

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

class CreateFTPDump(luigi.Task):
    runDate = luigi.DateParameter(default=datetime.datetime.now())
    ftp = FTPClient()

    def run(self):
        with self.output().open('w') as wddump:
            plist = {}
            plist['daily'] = self.ftp.listProductFiles('daily')
            plist['fiveDaily'] = self.ftp.listProductFiles('5day')
            plist['monthly'] = self.ftp.listProductFiles('monthly')

            json.dump(plist, wddump, indent=4, sort_keys=True, separators=(',', ':'))    

    
    def output(self):
       filePath = os.path.join(os.path.join(FILE_ROOT, self.runDate.strftime("%Y-%m-%d")), 'list.json')  

       return luigi.LocalTarget(filePath)