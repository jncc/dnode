import glob
import rsgislib
import os
from rsgislib import imageutils

ROOT_PATH = '/mnt/state/S2/'

InputBands1 = glob.glob(os.path.join(ROOT_PATH, '10m_mosaic/*.kea'))
outputImage1 = os.path.join(ROOT_PATH, 'Mosaic/10m.kea')
backgroundval = 0.
skipval = 0.
skipBand = 1
overlapBehaviour = 0
gdalformat = 'KEA'
datatype = rsgislib.TYPE_16UINT

rsgislib.imageutils.createImageMosaic(InputBands1,outputImage1, backgroundval, skipval, skipBand, overlapBehaviour, gdalformat, datatype)

InputBands2 = glob.glob(os.path.join(ROOT_PATH,'20m_mosaic/*.kea'))
outputImage2 = os.path.join(ROOT_PATH, 'Mosaic/20m.kea')

rsgislib.imageutils.createImageMosaic(InputBands2,outputImage2, backgroundval, skipval, skipBand, overlapBehaviour, gdalformat, datatype)