import glob
import rsgislib
from rsgislib import imageutils

InputBands1 = glob.glob('./S2/10m_mosaic/*.kea')
outputImage1 = './S2/Mosaic/10m.kea'
backgroundval = 0.
skipval = 0.
skipBand = 1
overlapBehaviour = 0
gdalformat = 'KEA'
datatype = rsgislib.TYPE_16UINT

rsgislib.imageutils.createImageMosaic(InputBands1,outputImage1, backgroundval, skipval, skipBand, overlapBehaviour, gdalformat, datatype)

InputBands2 = glob.glob('./S2/20m_mosaic/*.kea')
outputImage2 = './S2/Mosaic/20m.kea'

rsgislib.imageutils.createImageMosaic(InputBands2,outputImage2, backgroundval, skipval, skipBand, overlapBehaviour, gdalformat, datatype)