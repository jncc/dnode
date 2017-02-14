import datetime
import os
import json
import re

from ftplib import FTP
from config_manager import ConfigManager
from catalog_manager import CatalogManager

DAILY_FTP_ROOT = '/occci-v3.0/geographic/netcdf/daily/chlor_a/'
FIVEDAILY_FTP_ROOT = '/occci-v3.0/geographic/netcdf/5day/chlor_a/'
MONTHLY_FTP_ROOT = '/occci-v3.0/geographic/netcdf/monthly/chlor_a/'

class FTPClient:
    def __init__(self):
        self.config = ConfigManager('cfg.ini')
        self.ftp = FTP(self.config.getFtpHostname())
        self.ftp.login(self.config.getFtpUsername(), self.config.getFtpPassword())
        self.catalog = CatalogManager()

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

        res = {}

        for y in flist.keys():
            for f in flist[y]:
                if not self.catalog.exists(product, y + '/' + f):
                    m = re.search('OCx-([0-9]{6,8})-fv', f)
                    res[m.group(1)] = y + '/' + f

        return res

    def getFile(self, product, srcFile, target):
        if product == 'daily':
            pDir = DAILY_FTP_ROOT
        elif product == '5day':
            pDir = FIVEDAILY_FTP_ROOT
        elif product == 'monthly':
            pDir = MONTHLY_FTP_ROOT
        else:
            raise Exception('Unknown product')

        self.ftp.cwd(pDir)

        with open(target, 'wb') as t:
            self.ftp.retrbinary('RETR ' + srcFile, t.write)
