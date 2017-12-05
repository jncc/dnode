
# This script ingests S2 ARD images by looping through the S3 bucket.
# You need read access to S3, so you can use a user such as the "s3-read-only" user.
#
# To create a local security profile called 's3-read-only', run
# `aws configure --profile s3-read-only`
# add the key details, and then
# `python ingest.py --profile s3-read-only`.
# When hacking, use the --limit option to get just the first few S3 objects e.g.
# `python ingest.py --profile s3-read-only --limit 10`.


import argparse
import calendar
import boto3
import logging
import os
import re
import subprocess
import time
from types import SimpleNamespace


log = logging.getLogger('log')
regex = re.compile('Sentinel2([AB])\_((20[0-9]{2})([0-9]{2})([0-9]{2}))\/SEN2\_[0-9]{8}\_lat([0-9]{2,4})lon([0-9]{2,4})\_T([0-9]{2}[A-Z]{3})\_ORB([0-9]{3})\_(utm[0-9]{2}n)(\_osgb)?\_(clouds|sat|toposhad|valid|vmsk_sharp_rad_srefdem_stdsref|meta|thumbnail)(?!\.tif\.aux\.xml)')

def main():
    initialise_log()
    args = parse_command_line_args()

    log.info('Starting...')
    session = boto3.Session(profile_name=args.profile)
    s3c = session.client('s3')
    bucket = session.resource('s3').Bucket(args.bucket)
    log.info('Bucket is %s' % (args.bucket))

    output_by_date = {}
    output_by_grid = {}

    log.info('Scanning %s/%s...' % (args.bucket, args.path))
    for o in bucket.objects.filter(Prefix=args.path).limit(args.limit):
        match = regex.search(o.key)
        if match:
            log.info('Processing object %s' % (o.key))
            p = parse_object(match)
            print(p)
            add_by_date(output_by_date, o, p)
            add_by_grid(output_by_grid, o, p)
        else:
            log.info('Skipping object %s' % (o.key))


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
    parser = argparse.ArgumentParser(
        description='Runs through S3 directory looking for S2 ARD images and generates a html structure to view them in')
    parser.add_argument('-p', '--profile', type=str, required=True, help='Profile to use when connecting to S3')
    parser.add_argument('-b', '--bucket', type=str, required=False, default='eocoe-sentinel-2', help='S3 bucket to look in')
    parser.add_argument('-l', '--limit', type=int, required=False, default=1000000000, help='Limit the number of S3 objects scanned for dev')
    parser.add_argument('-a', '--path', type=str, required=False, default='initial', help='Folder within S3 bucket')
    parser.add_argument('-t', '--tempdir', type=str, required=False, default='./temp', help='Local temporary directory [Default: ./temp]')
    return parser.parse_args()

def sizeof_fmt(num, suffix='B'):
    """ Gets the human-readable file size. https://stackoverflow.com/a/1094933 """
    for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)

def parse_object(match):
    return SimpleNamespace(
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



def add_by_date(output, o, p):
    # make a data structure like
    # output[year][month][day][grid]['new_projection']
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
            'data': o.key,
            'size': sizeof_fmt(o.size)
        }
    else:
        output[p.year][p.month][p.day][p.grid][p.file_type] = {
            'data': o.key,
            'size': sizeof_fmt(o.size)
        }

def add_by_grid(output, o, p):
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
        output[p.grid][p.datestring]['new_projection']: p.new_projection

    if p.file_type == 'vmsk_sharp_rad_srefdem_stdsref':
        output[p.grid][p.datestring]['product'] = {
            'data': o.key,
            'size': sizeof_fmt(o.size)
        }
    else:
        output[p.grid][datestring][p.file_type] = {
            'data': o.key,
            'size': sizeof_fmt(o.size)
        }
    
def create_thumbnail(s3client, bucket, product, tempdir):
    s3client.download_file(bucket, product, os.path.join(tempdir, os.path.basename(product)))

    p = subprocess.Popen('gdal_translate -b 3 -b 2 -b 1 -ot Byte -of JPEG -outsize 5%% 5%% %s %s' % (os.path.join(tempdir, os.path.basename(product)), os.path.join(tempdir, os.path.basename(product).replace('_vmsk_sharp_rad_srefdem_stdsref.tif', '_thumbnail.jpg'))), shell=True)
    (output, err) = p.communicate()
    if output is not None:
        self.log.debug(output)
    if err is not None:
        raise RuntimeError(err)
    
    s3client.upload_file(os.path.join(tempdir, os.path.basename(product).replace('_vmsk_sharp_rad_srefdem_stdsref.tif', '_thumbnail.jpg')), bucket, product.replace('_vmsk_sharp_rad_srefdem_stdsref.tif', '_thumbnail.jpg'))


if __name__ == "__main__":
    main()
    exit()
        



#
#
#


    date_temp_path = os.path.join(args.tempdir, 'date')
    grid_temp_path = os.path.join(args.tempdir, 'grid')

    # Built output dictionary
    with open(os.path.join(date_temp_path, 'index.html'), 'w') as index:
        index.write('''<html>
    <head>
        <title>Sentinel 2 ARD Index</title>
        <style>
            td {
                border-bottom: 1px solid #bfc1c3;
                padding-bottom: 0.05em;
                text-align: center;
            }
        </style>        
    </head>
    <body>
        <h1>Sentinel-2 ARD Index</h1>
        <table width=80%>
        <thead>
            <td>Year</td>
            <td>Month</td>
        </thead>
        <tbody>''')
        
        for year in output_date:
            index.write('<tr>\n')
            index.write('<td width=25%%>%s</td>\n' % (year))
            index.write('<td>\n')
            for month in output_date[year]:
                index.write('<a href="%s/%s.html">%s</a><br/>\n' % (year, month, calendar.month_name[int(month)]))
                if not os.path.exists(os.path.join(date_temp_path, year)):
                    os.makedirs(os.path.join(date_temp_path, year))
                with open(os.path.join(os.path.join(date_temp_path, year), '%s.html' % month), 'w') as month_index:
                    month_index.write('<table>\n')
                    for day in output_date[year][month]:
                        for grid in output_date[year][month][day]:
                            month_index.write('<tr>\n')
                            # month_index.write('<td><img src="%s" height=100px width=100px></td>\n' % (output_date[year][month][day][grid]['thumbnail']['data']))
                            month_index.write('<td>\n')
                            month_index.write('<h3>%s</h3>\n' % (output_date[year][month][day][grid]['name']))
                            month_index.write('<a href="%s">Data [GeoTIFF] (%s)</a><br/>\n' % (output_date[year][month][day][grid]['product']['data'], output_date[year][month][day][grid]['product']['size']))
                            month_index.write('<a href="%s">Cloudmask [GeoTIFF] (%s)</a><br/>\n' % (output_date[year][month][day][grid]['clouds']['data'], output_date[year][month][day][grid]['clouds']['size']))
                            month_index.write('<a href="%s">Saturated pixel mask [GeoTIFF] (%s)</a><br/>\n' % (output_date[year][month][day][grid]['sat']['data'], output_date[year][month][day][grid]['sat']['size']))
                            month_index.write('<a href="%s">Valid pixel mask [GeoTIFF] (%s)</a><br/>\n' % (output_date[year][month][day][grid]['valid']['data'], output_date[year][month][day][grid]['valid']['size']))
                            month_index.write('<a href="%s">Topographic shadow mask [GeoTIFF] (%s)</a><br/>\n' % (output_date[year][month][day][grid]['toposhad']['data'], output_date[year][month][day][grid]['toposhad']['size']))
                            month_index.write('<a href="%s">Metadata [JSON] (%s)</a>\n' % (output_date[year][month][day][grid]['meta']['data'], output_date[year][month][day][grid]['meta']['size']))
                            month_index.write('</td>\n')
                            month_index.write('</tr>\n')
                    month_index.write('</table>\n')
            index.write('</td>\n')
            index.write('</tr>\n')
        index.write('</tbody></table></body></html>\n')

    with open(os.path.join(grid_temp_path, 'index.html'), 'w') as index:
        index.write('''<html>
    <head>
        <title>Sentinel 2 ARD Index</title>
        <style>
            td {
                border-bottom: 1px solid #bfc1c3;
                padding-bottom: 0.05em;
                text-align: center;
            }
        </style>        
    </head>
    <body>
        <h1>Sentinel-2 ARD Index</h1>
        <h2>MGRS Grids</h2>''')
        for grid in output_grid:
            index.write('<a href="%s/index.html">%s</a><br/>\n' % (grid, grid))
            if not os.path.exists(os.path.join(grid_temp_path, grid)):
                os.makedirs(os.path.join(grid_temp_path, grid))
            with open(os.path.join(os.path.join(grid_temp_path, grid), 'index.html'), 'w') as grid_index:
                grid_index.write('<h1>%s</h1>' % grid)
                grid_index.write('<table>\n')
                grid_index.write('<tr>\n')
                for date in output_grid[grid]:
                    grid_index.write('<td width=15%%>%s-%s-%s</td>\n' % (day, month, year))
                    # grid_index.write('<td><img src="%s" height=100px width=100px></td>\n' % (output_grid[grid][date]['thumbnail']['data']))
                    grid_index.write('<td>\n')
                    grid_index.write('<h3>%s</h3><br/>\n' % (output_grid[grid][date]['name']))
                    grid_index.write('<a href="%s">Data [GeoTIFF] (%s)</a><br/>\n' % (output_grid[grid][date]['product']['data'], output_grid[grid][date]['product']['size']))
                    grid_index.write('<a href="%s">Cloudmask [GeoTIFF] (%s)</a><br/>\n' % (output_grid[grid][date]['clouds']['data'], output_grid[grid][date]['clouds']['size']))
                    grid_index.write('<a href="%s">Saturated pixel mask [GeoTIFF] (%s)</a><br/>\n' % (output_grid[grid][date]['sat']['data'], output_grid[grid][date]['sat']['size']))
                    grid_index.write('<a href="%s">Valid pixel mask [GeoTIFF] (%s)</a><br/>\n' % (output_grid[grid][date]['valid']['data'], output_grid[grid][date]['valid']['size']))
                    grid_index.write('<a href="%s">Topographic shadow mask [GeoTIFF] (%s)</a><br/>\n' % (output_grid[grid][date]['toposhad']['data'], output_grid[grid][date]['toposhad']['size']))
                    grid_index.write('<a href="%s">Metadata [JSON] (%s)</a>\n' % (output_grid[grid][date]['meta']['data'], output_grid[grid][date]['meta']['size']))
                    grid_index.write('</td>\n')
                grid_index.write('</tr>\n')
                grid_index.write('</table>\n')

