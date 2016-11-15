
SentinelDownloadTask - using database
=====================================

Get recent available products
available = Intersect with the catalogue database (where location isblank and attempts < 20)
(Write a log with SentinelDownloadTask/20161114114704/files-to-download.txt)
Inserts new records or increments attempts count on existing records
Foreach file in available
    Increment attempt
    Download the file to S3
    Update the record with availability


Disadvantages:
lots of little database accesses to the live catalogue system over time
The changes to the catalogue database are not isolated to a single transaction
Ongoing database updates throughout the day.
Scalability (of catalogue database particularly



SentinelListTask
=================
Gets latest available.json e.g. SentinelListTask/2016-11-13/available.json 

Reads last ingestion date from available json
If this is older than 3 days, fail

Queries ESA for everything after last ingestion date

Queries catalogue database for intersection (NOT IN)

Makes new available.json for today

SentinelDownloadTask
====================
Foreach file in available
    Download the file to S3
    If fails
        log
    else 
        INSERT record into catalogue

