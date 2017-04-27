# Setting up an execution environment
## Development

### Create a database table for the downloader using the
- Create a posgres database table with the /setup/create_table.sql script 
- Create a downloader user and set read write permissions for it

### Create a working folder
Mount the working folder to the docker instance with -v ie -v /workingFolder:/mnt/state. See parameterisation info below.

## Run time

### Create a job status folder in S3
- Create a working folder in an S3 bucket used for luigi state
- Modify FILE_ROOT in workflow.py to point to this location
- Ensure that the aws credentials supplied on the command line have access

### Create a storage location for the downloaded data on S3
- Create a folder for the storage of the downloded Sentinal products.
- Ensure that the aws credentials supplied on the command line have access

### Create a database table for the downloader
Same as for development time execution.

### Modify config files
Specify the location of the central scheduler in luigi.conf
Configure the application search parameters and storage location in app.conf

# Docker image
## Parmeters

### debug flag
Specifying the --debug flag outputs the url that would have been requested only, does not add products to the catalog 

### seedDate flag
The seedDate is intened for initialising the system. The seed date is the earliest date from which data is requested from the ESA hub.  
E.g --seedDate 2014-01-01 would request all data that was ingested onto the ESA hub from the 1st January 2014

For each subsiquent run, the latest ingestion date from the previous run is used.

### Required
runDate:  
    The date for wich the downloader should be run.  
awsAccessKeyId:  
    The key id for the key used to write data to the S3 state and storage buckets. The storage bucket details are specified in the app.config.  
awsSecretKey:  
    The secret for the key identified by awsAccessKeyId
esaUsername:  
    The username used for access to search and download from the ESA scihub portal.  
esaPassword:  
    The password for the above username.  
dbHost:  
    The host name or ip of the server hosting the sentinel db.  
dbName:  
    The name of the sentinel db  
dbUser:  
    The username to connect to the database  
dbPassword:  
    The password for the above user  

-v local working folder:  
    Attatches the specified local folder to the /mnt/state folder in the docker instance. This folder is used for temporary working files such as partially downloaded products. It is also used to maintain application state rather then s3 when running with the --debug flag.

### Optional
debug:  
    Runs in test mode - stores state files in /mnt/state, doesn't make database changes, doesn't download files.  
seedDate:  
    Specifies the ingestion date for querying the esa portal. The ingestion date is the date the ESA portal ingested the product. For first run and isolated testing.  

## Config files
In /config

### app.cfg
#### EsaApi

Polygon = The WKT polygon that will be used to search the esa scihub portal for products
searchCriteria = Optional additional search paramters for the ESA search api. They are appended onto the default search criteria with an AND.

https://scihub.copernicus.eu/twiki/do/view/SciHubUserGuide/5APIsAndBatchScripting#Using_Open_Search_to_discover_pr

#### Amazon
bucketName = The name of the bucket in which to store downloaded files
destPath = The path in the buckete ie my/folder/here/x/y
region = The amazon region hosting the bucket

### luigi.conf
Configure the location of the central luigi scheduler daemon

## Build and run instructions

Build to image: 

    docker build -t esa-downloader .

Just run it:  

    docker run -v ~/local_working_folder:/mnt/state esa-downloader --runDate 2017-04-20 --awsAccessKeyId key_id --awsSecretKey secret --esaUsername esa_user --esaPassword esa_password --dbHost db.host.com --dbName dbname --dbUser dbuser --dbPassword dbpssword

Execute the job for the first time (user seed date):

    docker run -v ~/local_working_folder:/mnt/state esa-downloader --runDate 2017-04-20 --awsAccessKeyId key_id --awsSecretKey secret --esaUsername esa_user --esaPassword esa_password --dbHost db.host.com --dbName dbname --dbUser dbuser --dbPassword dbpssword --seedDate seedDate 


Run in isolated test mode:

    docker run -v ~/workfiles:/mnt/state  esa-downloader --runDate 2017-04-20 --seedDate 2016-12-01 --awsAccessKeyId na --awsSecretKey na --esaUsername esa_user --esaPassword esa_password --dbHost db.host.com --dbName dbname --dbUser dbuser --dbPassword dbpssword --debug --local-scheduler

Start interactivly:  

    docker run -it --entrypoint /bin/bash -v ~/workfiles:/mnt/state esa-downloader 

