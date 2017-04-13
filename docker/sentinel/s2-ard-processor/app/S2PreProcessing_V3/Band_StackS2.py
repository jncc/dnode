import rsgislib
import os
from rsgislib import imageutils

ROOT_PATH = '/mnt/state/S2'
gdalformat = 'KEA'
gdaltype = rsgislib.TYPE_16UINT



band1 = [1]
band2 = [2]
band3 = [3]
band4 = [4]
band5 = [5]
band6 = [6]

mosaic_path = os.path.join(ROOT_PATH, 'Mosaic/')
stack_path = os.path.join(ROOT_PATH, 'Stack/')

rsgislib.imageutils.selectImageBands(mosaic_path + '10m.kea',stack_path + '3.kea',gdalformat,gdaltype,band1)
rsgislib.imageutils.selectImageBands(mosaic_path + '10m.kea',stack_path + '2.kea',gdalformat,gdaltype,band2)
rsgislib.imageutils.selectImageBands(mosaic_path + '10m.kea',stack_path + '1.kea',gdalformat,gdaltype,band3)
rsgislib.imageutils.selectImageBands(mosaic_path + '10m.kea',stack_path + '8.kea',gdalformat,gdaltype,band4)
rsgislib.imageutils.selectImageBands(mosaic_path + '20m.kea',stack_path + '4.kea',gdalformat,gdaltype,band1)
rsgislib.imageutils.selectImageBands(mosaic_path + '20m.kea',stack_path + '5.kea',gdalformat,gdaltype,band2)
rsgislib.imageutils.selectImageBands(mosaic_path + '20m.kea',stack_path + '6.kea',gdalformat,gdaltype,band3)
rsgislib.imageutils.selectImageBands(mosaic_path + '20m.kea',stack_path + '7.kea',gdalformat,gdaltype,band4)
rsgislib.imageutils.selectImageBands(mosaic_path + '20m.kea',stack_path + '9.kea',gdalformat,gdaltype,band5)
rsgislib.imageutils.selectImageBands(mosaic_path + '20m.kea',stack_path + '10.kea',gdalformat,gdaltype,band6)

InputImages = [stack_path + '1.kea',stack_path + '2.kea',stack_path + '3.kea',stack_path + '4.kea',stack_path + '5.kea',stack_path + '6.kea',stack_path + '7.kea',stack_path + '8.kea',stack_path + '9.kea',stack_path + '10.kea']

outputImage = os.path.join(ROOT_PATH, 'ToARef.kea' )
#bandNamesList = ['Band1','Band2','Band3','Band4','Band5','Band6','Band7','Band8','Band9','Band10']
bandNames = ['Blue','Green','Red','RedEdge5','RedEdge6','RedEdge7','RedEdge8A','NIR','SWIR1','SWIR2']

rsgislib.imageutils.stackImageBands(InputImages, bandNames, outputImage, None, 0, gdalformat, gdaltype)

#rsgislib.imageutils.setBandNames(outputImage, bandNames)

rsgislib.imageutils.popImageStats(outputImage, True, 0., True)


