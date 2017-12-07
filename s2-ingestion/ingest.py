
# This script ingests S2 ARD images by looping through the object S3 bucket.
# You need read access to S3, so you can use a user such as the "s3-read-only" user.
#
# To create a local security profile called 's3-read-only', run
# `aws configure --profile s3-read-only`
# add the key details, and then
# `python ingest.py --profile s3-read-only`.


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

    output_by_product = {}
    output_by_date = {}
    output_by_grid = {}

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
                    log.info('Parsed %s' % (p))
                    add_by_product(output_by_product, p)
                    add_by_date(output_by_date, p)
                    add_by_grid(output_by_grid, p)
                else:
                    log.info('Skipping %s ...' % (line))

    write_json_file(output_by_product, args.outdir, 'by_product.json')
    write_json_file(output_by_date, args.outdir, 'by_date.json')
    write_json_file(output_by_grid, args.outdir, 'by_grid.json')

    # check_catalog_output()

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

def sizeof_fmt(num, suffix='B'):
    """ Gets the human-readable file size. https://stackoverflow.com/a/1094933 """
    for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)

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

def add_by_product(output, p):
    name = 'S2%s_%s%s%s_lat%slon%s_T%s_ORB%s_%s%s' % (p.satellite, p.year, p.month, p.day, p.lat, p.lon, p.grid, p.orbit, p.original_projection, ('_%s' % (p.new_projection) if p.new_projection is not None else ''))
    if not name in output:
        output[name] = []
    output[name].append(p.file_type)

def add_by_date(output, p):
    # make a data structure like
    # output[year][month][day][grid]['attributes-go-here']
    if not p.year in output:
        output[p.year] = {}
    if not p.month in output[p.year]:
        output[p.year][p.month] = {}
    if not p.day in output[p.year][p.month]:
        output[p.year][p.month][p.day] = {}
    if not p.grid in output[p.year][p.month][p.day]:
        output[p.year][p.month][p.day][p.grid] = {
            'name': 'S2%s_%s%s%s_lat%slon%s_T%s_ORB%s_%s%s' % (p.satellite, p.year, p.month, p.day, p.lat, p.lon, p.grid, p.orbit, p.original_projection, ('_%s' % (p.new_projection) if p.new_projection is not None else '')),
            'satellite': 'sentinel-2%s' % (p.satellite.lower()),
            'lat': p.lat,
            'lon': p.lon,
            'orbit': p.orbit,
            'original_projection': p.original_projection,
            'new_projection': p.original_projection       # Not a typo, this happens with Rockall
        }

        if p.new_projection is not None:
            output[p.year][p.month][p.day][p.grid]['new_projection']: p.new_projection

    if p.file_type == 'vmsk_sharp_rad_srefdem_stdsref':
        output[p.year][p.month][p.day][p.grid]['product'] = {
            'data': p.s3_key,
            'size': sizeof_fmt(p.s3_size)
        }
    else:
        output[p.year][p.month][p.day][p.grid][p.file_type] = {
            'data': p.s3_key,
            'size': sizeof_fmt(p.s3_size)
        }

def add_by_grid(output, p):
    if not p.grid in output:
        output[p.grid] = {}

    datestring = '%s%s%s' % (p.year, p.month, p.day)
    if not datestring in output[p.grid]:
        output[p.grid][datestring] = {
            'name': 'S2%s_%s%s%s_lat%slon%s_T%s_ORB%s_%s%s' % (p.satellite, p.year, p.month, p.day, p.lat, p.lon, p.grid, p.orbit, p.original_projection, ('_%s' % (p.new_projection) if p.new_projection is not None else '')),
            'satellite': 'sentinel-2%s' % (p.satellite.lower()),
            'lat': p.lat,
            'lon': p.lon,
            'orbit': p.orbit,
            'original_projection': p.original_projection,
            'new_projection': p.original_projection       # Not a typo, this happens with Rockall
        }

    if p.new_projection is not None:
        output[p.grid][datestring]['new_projection']: p.new_projection

    if p.file_type == 'vmsk_sharp_rad_srefdem_stdsref':
        output[p.grid][datestring]['product'] = {
            'data': p.s3_key,
            'size': sizeof_fmt(p.s3_size)
        }
    else:
        output[p.grid][datestring][p.file_type] = {
            'data': p.s3_key,
            'size': sizeof_fmt(p.s3_size)
        }

def write_json_file(data, outdir, filename):
    path = os.path.join('.', outdir, filename)
    with open(path, 'w') as f:
        json.dump(data, f, indent=4)    

# def check_catalog_output():


def create_thumbnail(s3client, bucket, product, outdir):
    s3client.download_file(bucket, product, os.path.join(outdir, os.path.basename(product)))

    p = subprocess.Popen('gdal_translate -b 3 -b 2 -b 1 -ot Byte -of JPEG -outsize 5%% 5%% %s %s' % (os.path.join(outdir, os.path.basename(product)), os.path.join(outdir, os.path.basename(product).replace('_vmsk_sharp_rad_srefdem_stdsref.tif', '_thumbnail.jpg'))), shell=True)
    (output, err) = p.communicate()
    if output is not None:
        self.log.debug(output)
    if err is not None:
        raise RuntimeError(err)
    
    s3client.upload_file(os.path.join(outdir, os.path.basename(product).replace('_vmsk_sharp_rad_srefdem_stdsref.tif', '_thumbnail.jpg')), bucket, product.replace('_vmsk_sharp_rad_srefdem_stdsref.tif', '_thumbnail.jpg'))

main()
    