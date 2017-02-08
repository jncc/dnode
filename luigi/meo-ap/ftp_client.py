import datetime
import os
import json

from ftplib import FTP
from config_manager import ConfigManager

DAILY_FTP_ROOT = '/occci-v3.0/geographic/netcdf/daily/chlor_a/'
FIVEDAILY_FTP_ROOT = '/occci-v3.0/geographic/netcdf/5day/chlor_a/'
MONTHLY_FTP_ROOT = '/occci-v3.0/geographic/netcdf/monthly/chlor_a/'

class FTPClient:
    def __init__(self):
        self.config = ConfigManager('cfg.ini')
        self.ftp = FTP(self.config.getFtpHostname())
        self.ftp.login(self.config.getFtpUsername(), self.config.getFtpPassword())

    def listProductFiles(self, product):
        flist = {}
        ylist = []

        if product == 'daily':
            pDir = DAILY_FTP_ROOT
        elif product == '5day':
            pDir = FIVEDAILY_FTP_ROOT
        elif product == 'monthly':
            pDir = MONTHLY_FTP_ROOT
        else:
            raise Exception('Unknown product')

        self.ftp.cwd(pDir)
        self.ftp.retrlines('NLST', ylist.append)

        for yearDir in ylist:
            flist[yearDir] = []
            self.ftp.cwd(pDir + yearDir + '/')
            self.ftp.retrlines('NLST', flist[yearDir].append)

        res = []

        for y in flist.keys():
            for f in flist[y]:
                res.append(y + '/' + f)

        return res

