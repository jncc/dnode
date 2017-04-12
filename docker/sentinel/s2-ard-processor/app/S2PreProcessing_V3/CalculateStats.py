import os.path
import sys
import rsgislib
from rsgislib import imageutils

inDir = '/mnt/state/S2'

files = [file for file in os.listdir(inDir) if file.endswith('.tif')]

for file in files:
    rsgislib.imageutils.popImageStats(os.path.join(inDir, file), True, 0., True)


