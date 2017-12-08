
# This script parses S2 ARD products by looping through local object listing from the S3 bucket.

import argparse
import json
import logging
import os
import re
import subprocess
import sys
from types import SimpleNamespace


log = logging.getLogger('log')

validation_regex = re.compile('^(.+) (\d+)$')

# regex to extract information from the S3 object key
#   starts  /RockallSentinel2A, /UKSentinel2A or /UKSentinel2B
#                              |            full_date                                                                                                                              new_projection (optional)
#                              | satellite(A|B) |  also year, month, day                              lat            lon              grid               orbit   original_projection     |     file_type (one of several)              this file extension group is not used? (negative lookahead?)
#                              |        |       |            |                                         |              |                |                   |              |              |       |                                                                        |
# example match             /UKSentinel2A      _20170531     |                     /SEN2_20170531  _lat60          lon023          _T30VXM              _ORB123        _utm30n          _osgb   _vmsk_sharp_rad_srefdem_stdsref                                           .tif
extraction_regex = re.compile('Sentinel2([AB])\_((20[0-9]{2})([0-9]{2})([0-9]{2}))\/SEN2\_[0-9]{8}\_lat([0-9]{2,4})lon([0-9]{2,4})\_T([0-9]{2}[A-Z]{3})\_ORB([0-9]{3})\_(utm[0-9]{2}n)(\_osgb)?\_(clouds|sat|toposhad|valid|vmsk_sharp_rad_srefdem_stdsref|meta|thumbnail)(?!\.tif\.aux\.xml)')

def main():
    initialise_log()
    args = parse_command_line_args()

    log.info('Starting...')

    output = {}

    log.info('Scanning input file %s...' % (args.input))

    with open(args.input) as f:
        for line in f:
            line = line.rstrip('\n') # remove newline
            validation_match = validation_regex.match(line)
            if validation_match is None:
                sys.exit('Uh oh, input line was bad.')
            else:
                extraction_match = extraction_regex.search(line)
                if extraction_match:
                    log.info('Processing %s ...' % (line))
                    p = parse_object(validation_match, extraction_match)
                    # log.info('Parsed %s' % (p))
                    add_object_by_product(output, p)
                else:
                    log.info('Skipping %s ...' % (line))

    write_json_file(output, args.outdir, 'parsed-%s.json' % (os.path.basename(args.input)))

def initialise_log():
    # formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    formatter = logging.Formatter('%(levelname)s - %(message)s')
    log.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    ch.setFormatter(formatter)
    # fh = logging.FileHandler('s2init-%s.log' % time.strftime('%y%m%d-%H%M%S'))
    # fh.setFormatter(formatter)
    # fh.setLevel(logging.DEBUG)
    log.addHandler(ch)
    # log.addHandler(fh)
    log.info('Logger initialised.')

def parse_command_line_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input', type=str, required=True, help='File of S3 objects')
    parser.add_argument('-p', '--path', type=str, required=False, default='initial', help='Folder within S3 bucket')
    parser.add_argument('-o', '--outdir', type=str, required=False, default='output', help='Local output directory [Default: ./output]')
    return parser.parse_args()

def parse_object(validation_match, match):
    return SimpleNamespace(
        s3_key=              validation_match.group(1),
        s3_size=             int(validation_match.group(2)),
        satellite=           match.group(1),
        full_date=           match.group(2),
        year=                match.group(3),
        month=               match.group(4),
        day=                 match.group(5),
        lat=                 match.group(6),
        lon=                 match.group(7),
        grid=                match.group(8),
        orbit=               match.group(9),
        original_projection= match.group(10),
        new_projection=      match.group(11), # Optional Group, could give None
        file_type=           match.group(12),
    )

def add_object_by_product(output, p):

    product = 'S2%s_%s%s%s_lat%slon%s_T%s_ORB%s_%s%s' % (p.satellite, p.year, p.month, p.day, p.lat, p.lon, p.grid, p.orbit, p.original_projection, ('_%s' % (p.new_projection) if p.new_projection is not None else ''))
    if not product in output:
        output[product] = {}

    attrs = {
        'full_date': p.full_date,
        'year': p.year,
        'month': p.month,
        'day': p.day,
        'grid': p.grid,
        'satellite': 'sentinel-2%s' % (p.satellite.lower()),
        'lat': p.lat,
        'lon': p.lon,
        'orbit': p.orbit,
        'original_projection': p.original_projection,
        'new_projection': p.new_projection if p.new_projection is not None else p.original_projection 
    }
    output[product]['attrs'] = attrs

    if not 'files' in output[product]:
        output[product]['files'] = []
    file_type = 'product' if p.file_type == 'vmsk_sharp_rad_srefdem_stdsref' else p.file_type
    output[product]['files'].append({
        'type': file_type,
        'data': p.s3_key,
        'size_in_bytes': p.s3_size,
        'size': sizeof_fmt(p.s3_size),
    })

def write_json_file(data, outdir, filename):
    path = os.path.join('.', outdir, filename)
    with open(path, 'w') as f:
        json.dump(data, f, indent=4)    

def sizeof_fmt(num, suffix='B'):
    """ Gets the human-readable file size. https://stackoverflow.com/a/1094933 """
    for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)

main()
    