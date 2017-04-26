# Running the downloader 
## Create a job status folder in S3
- Create a working folder in an S3 bucket used for luigi state
- Modify FILE_ROOT in workflow.py to point to this location
- Ensure the default credentials for the aws cli permit access to this bucket. (aws configure)

## Create a storage location for the downloaded data on S3
- Create a folder for the storage of the downloded Sentinal products.

## Create a database table for the downloader using the
- Create a posgres database table with the /setup/create_table.sql script 
- Create a downloader user and set read write permissions for it

## Create the downloader configuration
- Edit app.cfg
- Create a database connection string
- Add the location of the S3 bucket used for storing downloaded data

bucketName is the simply the target bucket name ie "myBucket"
destPath is the path excluding the bucket name ie "myFolder"

An s3 path such as s3://myBucket/myFolder/mySubfolder would go into the cfg.ini as
bucketName = myBucket
destPath = /myFolder/mySubfolder

## Execute the job for the fist time
The job must be run manually in the first instance by specifying a seed date. 

The seed date is the earliest date from which data is requested from the ESA hub.
E.g --seedDate 2014-01-01 would request all data that was ingested onto the ESA hub from that date.

For each subsiquent run, the latest ingestion date from the previous run is used.

PYTHONPATH='.' luigi --module workflow DownloadAvailableProducts --local-scheduler --runDate <todays date in yyyy-mm-dd> --seedDate <seed date in yyyy-mm-dd>

## Execute the job
- Ensure the luigi environment is sourced from the parent folder
```
source luigi_venv/bin/activate
```
- Execute the downloader with the following command, substiuting the --runDate 
```
PYTHONPATH='.' luigi --module workflow DownloadAvailableProducts --local-scheduler --runDate 2017-04-20 --seedDate 2016-12-19
```
## debug flag
Specifying the --debug flag outputs the url that would have been requested only,
NB: USE WITH CAUTION this WILL update the catalog as if the product had been downloaded

## seedDate flag
The seedDate is intened for initialising the system. It is the earliest ESA ingestion date from which the system should begin downloading ata

# Docker image
## Parmeters
debug:
    If supplied 
seedDate
runDate
awsAccessKeyId
awsSecretKey
esaUsername
esaPassword

## Build and run instructions

Build to image: 

    docker build -t esa-downloader .

Just run it:

    docker run -v ~/workfiles:/mnt/state  esa-downloader --rundate 2017-04-20

