#!/bin/bash
python /app/s3download.py
cd S2PreProcessing_V3
source activate osgeoenv
export CPL_ZIP_ENCODING=UTF-8
sh ./Pre_Processing.sh
source deactivate osgeoenv
phython /app/s3upload.py
