#!/bin/bash
conda create --name osgeoenv python=3.5 
source activate osgeoenv;conda install -c conda-forge gdal 
conda install -c conda-forge tuiview 
conda install -c conda-forge -c rios rsgislib 
conda install -c conda-forge -c rios rios 
conda install -c conda-forge -c rios arcsi 
conda install -c conda-forge scikit-learn