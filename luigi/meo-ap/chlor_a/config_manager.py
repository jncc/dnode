import sys
import os
import ConfigParser as configparser

class ConfigManager:

    def __init__(self, configuration_file):
        if configuration_file is None or (len(configuration_file) and not os.path.isfile(configuration_file)):
            raise Exception("Missing configuration file")

        try: 
            self.config = configparser.ConfigParser()
            self.config.read([configuration_file, os.path.expanduser('~/.meo-ap.cfg')])
        except configparser.Error, e:
            msg = "Error parsing configuration file: %s" & (e,)
            raise Exception(msg)

    def getFtpHostname(self):
        return self.config.get('FTP', 'hostname')

    def getFtpUsername(self):
        return self.config.get('FTP', 'username')

    def getFtpPassword(self):
        return self.config.get('FTP', 'password')

    def getAnnualFolder(self):
        return self.config.get('Local', 'folder')

    def getDatabaseConnectionString(self):
        return self.config.get('Database', 'connection')

    def getAmazonKeyId(self):
        return self.config.get('Amazon', 'accessKeyId')

    def getAmazonKeySecret(self):
        return self.config.get('Amazon', 'accessKeySecret')

    def getAmazonDestPath(self):
        return self.config.get('Amazon', 'destPath')

    def getAmazonRegion(self):
        return self.config.get('Amazon', 'region')

    def getAmazonBucketName(self):
        return self.config.get('Amazon', 'bucketName')

        