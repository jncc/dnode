# Sentinel 1 ARD Backscatter Product downloader

This workflow determines what products are available from a remote repository of processed Sentinel-1 ARD (Backscatter) products

## Requirements

I recommend that you install the python-gdal (or python3-gdal) packages rather than rely on pip installing the GDAL bindings so installation should go something like;

```
virtualenv -p python3 --system-site-packages venv
source ./venv/bin/activate
pip install -r requirements.txt
```
 
## Running

If you want to run this by hand you will have to use the following command;

```
#!/bin/bash
PYTHONPATH='.' luigi --module workflow CreateProductList --local-scheduler --debug
```


## Other Documentation

Find some more detailed documentation in the [wiki](https://github.com/jncc/dnode/wiki/S1-ARD-Backscatter-Downloader)
