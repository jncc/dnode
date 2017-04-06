#!/bin/bash
source activate osgeoenv
export CPL_ZIP_ENCODING=UTF-8
/app/s3download
sh ./S2PreProcessing_V3/Pre_Processing.sh
/app/s3upload
