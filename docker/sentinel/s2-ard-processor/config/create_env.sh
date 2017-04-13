#!/bin/bash
conda create --name osgeoenv python=3.5 
source activate osgeoenv
conda install -c conda-forge gdal 
conda install -c conda-forge -c rios rsgislib 
conda install -c conda-forge -c rios rios 

# Fix for the following lib dependency problem, may not be needed for future build
# https://groups.google.com/forum/#!topic/rsgislib-support/YD_sTMTxAis
conda update -c au-eoed -c rios -c conda-forge --all
