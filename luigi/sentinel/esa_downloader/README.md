# Running the downloader 
## Create a job status folder in S3
- Create a working folder in an S3 bucket
- Modify FILE_ROOT in workflow.py to point to this location

## Create a seed status file
- Copy ./seed/available.json into this location.
- Create a folder in this location with yesterdays date. ie  S3bucket/workflow/2016-12-11
- Copy available.json into this folder

## Execute the job
- Ensure the luigi environment is sourced from the parent folder
```
source luigi_venv/bin/activate
```
- Execute the downloader with the following command, substiuting the --runDate 
```
PYTHONPATH='.' luigi --module workflow DownloadAvailableProducts --local-scheduler --runDate 2016-11-28
```
## debug flag
Specifying the --debug flag prevents lengthy downloads, outputs the url that would have been requested only,
NB: USE WITH CAUTION this WILL update the catalog as if the product had been downloaded

## seedDate flag
The seedDate is intened for initialising the system. It is the ESA ingestion date from which the system should begin downloading.

