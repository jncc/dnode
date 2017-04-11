import rsgislib
from rsgislib import imageutils

gdalformat = 'KEA'
gdaltype = rsgislib.TYPE_16UINT

band1 = [1]
band2 = [2]
band3 = [3]
band4 = [4]
band5 = [5]
band6 = [6]
rsgislib.imageutils.selectImageBands('./S2/Mosaic/10m.kea','./S2/Stack/3.kea',gdalformat,gdaltype,band1)
rsgislib.imageutils.selectImageBands('./S2/Mosaic/10m.kea','./S2/Stack/2.kea',gdalformat,gdaltype,band2)
rsgislib.imageutils.selectImageBands('./S2/Mosaic/10m.kea','./S2/Stack/1.kea',gdalformat,gdaltype,band3)
rsgislib.imageutils.selectImageBands('./S2/Mosaic/10m.kea','./S2/Stack/8.kea',gdalformat,gdaltype,band4)
rsgislib.imageutils.selectImageBands('./S2/Mosaic/20m.kea','./S2/Stack/4.kea',gdalformat,gdaltype,band1)
rsgislib.imageutils.selectImageBands('./S2/Mosaic/20m.kea','./S2/Stack/5.kea',gdalformat,gdaltype,band2)
rsgislib.imageutils.selectImageBands('./S2/Mosaic/20m.kea','./S2/Stack/6.kea',gdalformat,gdaltype,band3)
rsgislib.imageutils.selectImageBands('./S2/Mosaic/20m.kea','./S2/Stack/7.kea',gdalformat,gdaltype,band4)
rsgislib.imageutils.selectImageBands('./S2/Mosaic/20m.kea','./S2/Stack/9.kea',gdalformat,gdaltype,band5)
rsgislib.imageutils.selectImageBands('./S2/Mosaic/20m.kea','./S2/Stack/10.kea',gdalformat,gdaltype,band6)

InputImages = ['./S2/Stack/1.kea','./S2/Stack/2.kea','./S2/Stack/3.kea','./S2/Stack/4.kea','./S2/Stack/5.kea','./S2/Stack/6.kea','./S2/Stack/7.kea','./S2/Stack/8.kea','./S2/Stack/9.kea','./S2/Stack/10.kea']

outputImage = './S2/ToARef.kea'
#bandNamesList = ['Band1','Band2','Band3','Band4','Band5','Band6','Band7','Band8','Band9','Band10']
bandNames = ['Blue','Green','Red','RedEdge5','RedEdge6','RedEdge7','RedEdge8A','NIR','SWIR1','SWIR2']

rsgislib.imageutils.stackImageBands(InputImages, bandNames, outputImage, None, 0, gdalformat, gdaltype)

#rsgislib.imageutils.setBandNames(outputImage, bandNames)

rsgislib.imageutils.popImageStats(outputImage, True, 0., True)


