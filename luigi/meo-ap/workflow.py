import luigi
import docker
import datetime
import os
import json

from ftplib import FTP

from luigi.util import requires

#FILE_ROOT = 's3://jncc-data/workflows/s2ard/'
FILE_ROOT = '/tmp/meo-ap'
DAILY_FTP_ROOT = '/occci-v3.0/geographic/netcdf/daily/chlor_a/'
FIVEDAILY_FTP_ROOT = '/occci-v3.0/geographic/netcdf/5day/chlor_a/'
MONTHLY_FTP_ROOT = '/occci-v3.0/geographic/netcdf/monthly/chlor_a/'
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
    ftp = FTP('ftp.rsg.pml.ac.uk')
    ftp.login('oc-cci-data', 'ELaiWai8ae')

    def run(self):
        with self.output().open('w') as wddump:
            plist = {}
            plist['daily'] = self.listProductFiles(DAILY_FTP_ROOT)
            plist['fiveDaily'] = self.listProductFiles(FIVEDAILY_FTP_ROOT)
            plist['monthly'] = self.listProductFiles(MONTHLY_FTP_ROOT)

            json.dump(plist, wddump, indent=4, sort_keys=True, separators=(',', ':'))    

    def listProductFiles(self, pDir):
        flist = {}
        ylist = []
        self.ftp.cwd(pDir)
        self.ftp.retrlines('NLST', ylist.append)

        for yearDir in ylist:
            flist[yearDir] = []
            self.ftp.cwd(pDir + yearDir + '/')
            self.ftp.retrlines('NLST', flist[yearDir].append)

        return flist
    
    def output(self):
       filePath = os.path.join(os.path.join(FILE_ROOT, self.runDate.strftime("%Y-%m-%d")), 'list.json')  

       return luigi.LocalTarget(filePath)