import sys
import os
import ConfigParser as configparser

class ConfigManager:

    def __init__(self, configuration_file):
        if configuration_file is None or (len(configuration_file) and not os.path.isfile(configuration_file)):
            raise Exception("Missing configuration file")

        try: 
            self.config = configparser.ConfigParser()
            self.config.read([configuration_file])
        except configparser.Error, e:
            msg = "Error parsing configuration file: %s" & (e,)
            raise Exception(msg)

    def get_esa_credentials(self):
        username = self.config.get('EsaApi','username')
        password = self.config.get('EsaApi','password')

        return username + ':' + password

    def get_esa_searchCriteria(self):
        return self.config.get('EsaApi','searchCriteria', fallback=None)

    def get_search_polygon(self):
        return self.config.get('EsaApi','polygon')

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

        