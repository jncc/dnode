#!/bin/bash
source activate osgeoenv
export CPL_ZIP_ENCODING=UTF-8
python /app/s3download
cd S2PreProcessing_V3
sh ./Pre_Processing.sh
phython /app/s3upload
