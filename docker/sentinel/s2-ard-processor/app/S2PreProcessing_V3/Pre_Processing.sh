#!/bin/sh

#  Pre_Processing.sh
#  
#
#  Created by Gwawr Angharad Jones on 21/07/2016.
#  Updated 12/09/2016.
#  Felixs hacked up version

##### Extract Sentinel-2 data in a usable format #####
# Manually change name of zip file #
mkdir /mnt/state/S2
python extract_s2_data.py -o /mnt/state/S2 --of KEA \ /mnt/state/input.zip

##### Cloud Mask - NEED TO TEST FMASK FOR SENTINEL-2 #####

##### Reproject to OSGB #####
# Bands with 10m resolution and UTM29 zone source projection
for f in /mnt/state/S2/*10m_UTM29N.kea; do echo "Processing $f"; gdalwarp -of KEA -s_srs EPSG:32629 -t_srs "+proj=tmerc +lat_0=49 +lon_0=-2 +k=0.999602717 +x_0=400000 +y_0=-100000 +ellps=airy +units=m +no_defs +nadgrids=./OSTN02_NTv2.gsb" -tr 10 10 $f ${f%.kea}_OSGB.kea; done

# Bands with 20m resolution and UTM29 zone source projection
for f in /mnt/state/S2/*20m_UTM29N.kea; do echo "Processing $f"; gdalwarp -of KEA -s_srs EPSG:32629 -t_srs "+proj=tmerc +lat_0=49 +lon_0=-2 +k=0.999602717 +x_0=400000 +y_0=-100000 +ellps=airy +units=m +no_defs +nadgrids=./OSTN02_NTv2.gsb" -tr 20 20 $f ${f%.kea}_OSGB.kea; done

# Bands with 10m resolution and UTM30 zone source projection
for f in /mnt/state/S2/*10m_UTM30N.kea; do echo "Processing $f"; gdalwarp -of KEA -s_srs EPSG:32630 -t_srs "+proj=tmerc +lat_0=49 +lon_0=-2 +k=0.999602717 +x_0=400000 +y_0=-100000 +ellps=airy +units=m +no_defs +nadgrids=./OSTN02_NTv2.gsb" -tr 10 10 $f ${f%.kea}_OSGB.kea; done

# Bands with 20m resolution and UTM30 zone source projection
for f in /mnt/state/S2/*20m_UTM30N.kea; do echo "Processing $f"; gdalwarp -of KEA -s_srs EPSG:32630 -t_srs "+proj=tmerc +lat_0=49 +lon_0=-2 +k=0.999602717 +x_0=400000 +y_0=-100000 +ellps=airy +units=m +no_defs +nadgrids=./OSTN02_NTv2.gsb" -tr 20 20 $f ${f%.kea}_OSGB.kea; done

# Bands with 10m resolution and UTM31 zone source projection
for f in /mnt/state/S2/*10m_UTM31N.kea; do echo "Processing $f"; gdalwarp -of KEA -s_srs EPSG:32631 -t_srs "+proj=tmerc +lat_0=49 +lon_0=-2 +k=0.999602717 +x_0=400000 +y_0=-100000 +ellps=airy +units=m +no_defs +nadgrids=./OSTN02_NTv2.gsb" -tr 10 10 $f ${f%.kea}_OSGB.kea; done

# Bands with 20m resolution and UTM31 zone source projection
for f in /mnt/state/S2/*20m_UTM31N.kea; do echo "Processing $f"; gdalwarp -of KEA -s_srs EPSG:32631 -t_srs "+proj=tmerc +lat_0=49 +lon_0=-2 +k=0.999602717 +x_0=400000 +y_0=-100000 +ellps=airy +units=m +no_defs +nadgrids=./OSTN02_NTv2.gsb" -tr 20 20 $f ${f%.kea}_OSGB.kea; done

##### Resample 20m bands to 10m #####
for f in /mnt/state/S2/*20m*_OSGB.kea; do gdal_translate -of KEA -outsize 200% 200% $f ${f%.kea}_RS.kea; done

##### Move processed files to another folder for mosaicing and remove unwanted files #####
mkdir /mnt/state/S2/10m_mosaic
for f in /mnt/state/S2/*10m*OSGB.kea; do mv $f /mnt/state/S2/10m_mosaic; done

mkdir /mnt/state/S2/20m_mosaic
for f in /mnt/state/S2/*20m*RS.kea; do mv $f /mnt/state/S2/20m_mosaic; done

##### Mosaic Granules per Band #####
mkdir /mnt/state/S2/Mosaic
python Mosaic_Bands.py

##### Stack Bands #####
# Need to Manually change end file name
mkdir /mnt/state/S2/Stack
python Band_StackS2.py

##### Apply Dark Object Subtraction Method #####
python DOS.py

##### Translate final image to a Geotiff #####
# Manually change name of file #
for f in /mnt/state/S2/SRef.kea; do echo "Processing $f"; gdal_translate -of GTiff -co "COMPRESS=LZW" -co "TILED=YES" -co "BLOCKXSIZE=256" -co "BLOCKYSIZE=256" -co "BIGTIFF=YES" $f /mnt/state/S2/output.tif; done

##### Calculate Stats on Tiff file #####
#for f in /mnt/state/S2/*.tif; do echo "Processing $f"; gdaladdo -r average $f 2 4 8 16; done
python CalculateStats.py

##### Set Bands and WKT #####
python SetBands_wkt.py

##### Calculate Products (NDVI and NDWI) #####
#mkdir ./Intermediate_Products
#python CalcProducts.py

##### Move output file to output location
mv /mnt/state/S2/S2_OUTPUT.tif /mnt/state/output.tif

