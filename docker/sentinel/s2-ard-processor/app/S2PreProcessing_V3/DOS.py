import rsgislib
from rsgislib import imagecalc
from rsgislib import imagecalibration


inputFile = './S2/ToARef.kea'
outputFile = './S2/SRef.kea'
darkObjPercentile = 0.01
noDataVal = 0
gdalformat = 'KEA'
#outDataType = rsgislib.TYPE_16UINT
nonNegative = True
darkObjReflVal = 0

rsgislib.imagecalibration.performDOSCalc(inputFile, outputFile, gdalformat, nonNegative, noDataVal, darkObjReflVal, darkObjPercentile, copyBandNames=True, calcStatsPyd=False)

#A command to perform a dark object subtraction (DOS) on an input image.
        
        #       * inputFile - input image to which the DOS method is to be applied. Typically, this image with be in top of atmosphere reflectance (TOA)
        #     * outputFile - the output image file
        #      * gdalFormat - the output image file format (default = KEA)
        #      * nonNegative - is a boolean specifying where negative output pixel values will be accepted (Dafualt is True; i.e., no negative values)
        #      * noDataVal - is the no data value within the input image file.
        #     * darkObjReflVal - is an offset which is applied to all pixel values to make a minimum reflectance value (Default = 0)
        #    * darkObjPercentile - is the percentile of the input image used to define the dark object threshold, range is 0 - 1 (Default is 0.01; i.e., 1%).
        #  * copyBandNames - is a boolean specifying that the band names of the input image should be copied to the output image file (Default: True)
#  * calcStatsPyd - is a boolean specifying that the image stats and pyramids should be calculated on the output image (Default: True)
