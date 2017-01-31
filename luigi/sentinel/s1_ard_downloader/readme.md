# Sentinel 1 ARD Backscatter Product downloader

This workflow determines what products are available from a remote repository of processed Sentinel-1 ARD (Backscatter) products

## Workflow Diagrams



## File Formats

### Current Format of files from remote repo

- `S1A<IMAGE ID>.SAFE.data`  (image folder named according to SNAP naming convention)
  - `S1A<IMAGE ID>.tiff` (data file)
  - `S1A<IMAGE ID>_metadata.xml`  (metadata file)
  - `S1A<IMAGE ID>_quicklook.jpeg` (preview file)
  - `/Footprint`  (folder containing footprint shapefile)
    - `S1A<IMAGE ID>_footprint.dbf`
    - `S1A<IMAGE ID>_footprint.shx`
    - `S1A<IMAGE ID>_footprint.shp`
    - `S1A<IMAGE ID>_footprint.prj`
  - `/OSNI1952` (Folder that is present only if Northern Ireland data exists)
    - `S1A<IMAGE ID>_OSNI1952.tiff`  (Northern Ireland data file)
    - `S1A<IMAGE ID>_OSNI1952_metadata.xml`  (Northern Ireland metadata file)
    - `S1A<IMAGE ID>_OSNI1952_quicklook.jpeg` (Northern Ireland preview file)
    - `/Footprint`  (folder containing footprint shapefile)
      - `S1A<IMAGE ID>_OSNI1952_footprint.dbf`
      - `S1A<IMAGE ID>_OSNI1952_footprint.shx`
      - `S1A<IMAGE ID>_OSNI1952_footprint.shp`
      - `S1A<IMAGE ID>_OSNI1952_footprint.prj`

### Target Format of files on S3

Files will be stored on our S3 repository with the following structure, a basic interface to this data will be provided at a later date and an itermidiate access page will be created to allow navigation of the data in the short term

Only real changes are the change of the footprint files to geojson and the addition of a wgs84 footprint, this will be programatically accessible at a later point through a catalog

- `S1A<IMAGE ID>.SAFE.data`  (image folder named according to SNAP naming convention)
  - `S1A<IMAGE ID>.tiff`  (data file)
  - `S1A<IMAGE ID>_metadata.xml`  (metadata file)
  - `S1A<IMAGE ID>_quicklook.jpeg` (preview file)
  - `/Footprint`  (folder containing footprint shapefile)
    - `S1A<IMAGE ID>_footprint.geojson`
    - `S1A<IMAGE ID>_footprint_wgs84.geojson`
  - `/OSNI1952` (Folder that is present only if Northern Ireland data exists)
    - `S1A<IMAGE ID>_OSNI1952.tiff`  (Northern Ireland data file)
    - `S1A<IMAGE ID>_OSNI1952_metadata.xml`  (Northern Ireland metadata file)
    - `S1A<IMAGE ID>_OSNI1952_quicklook.jpeg` (Northern Ireland preview file)
    - `/Footprint`  (folder containing footprint shapefile)
      - `S1A<IMAGE ID>_OSNI1952_footprint.geojson`
      - `S1A<IMAGE ID>_OSNI1952_footprint_wgs84.geojson`
