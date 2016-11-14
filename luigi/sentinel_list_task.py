import luigi
from luigi.s3 import S3Target

class YesterdaysAvailableListTask(luigi.ExternalTask):    
    yesterday = luigi.Parameter()
    def output(self):
	    return S3Target('s3://jncc-data/sentinel-' + yesterday + 'example/hello.txt')
    

class SentinelListTask(luigi.Task)


# SentinelListTask
# ================
# SentinelListTask/2016-11-13/available.json  (files to download)
# query esa hub for all products ingested after latest ingestion date (in available.ljson)

# SentinelDownloadTask
# ====================
# write a properties.json with attempts=0 
# downloads the file to S3
# if succeeds, adds the properties and location to the properties.json 
# SentinelDownloadTask/2016-11-13/S1A_IW_GRDH_1SDV_20151116T062140_20151116T062205_008626_00C401_1F80.json
# all successfully downloaded files properties.json (plus s3 path location)


# SentinelListTask/2016-11-14/available.json  (files to download)


