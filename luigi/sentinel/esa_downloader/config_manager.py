import sys
import os
import ConfigParser as configparser

class ConfigManager:

    def __init__(self, configuration_file):
        if configuration_file is None or (len(configuration_file) and not os.path.isfile(configuration_file)):
            raise Exception("Missing configuration file")

        try: 
            self.config = configparser.ConfigParser()
            self.config.read([configuration_file, os.path.expanduser('~/.scihub.cfg')])
        except configparser.Error, e:
            msg = "Error parsing configuration file: %s" & (e,)
            raise Exception(msg)

    def get_esa_credentials(self):
        username = self.config.get('EsaCredentials','username')
        password = self.config.get('EsaCredentials','password')

        return username + ':' + password
    
    def get_temp_path(self):
        return self.config.get('DownloaderConfig','tempPath')
        