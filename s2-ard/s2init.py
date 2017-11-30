import argparse
import calendar
import boto3
import logging
import os
import re
import subprocess
import time

def sizeof_fmt(num, suffix='B'):
    for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)

def add_item(output, matched, satellite, year, month, day, lat, lon, grid, orbit, original_projection, new_projection, file_type, remote, year_slicing=True):
    if year_slicing:
        if not year in output:
            output[year] = {}
        if not month in output[year]:
            output[year][month] = {}
        if not day in output[year][month]:
            output[year][month][day] = {}
        if not grid in output[year][month][day]:
            output[year][month][day][grid] = {
                'name': 'S2%s_%s%s%s_lat%slon%s_T%s_ORB%s_%s%s' % (satellite, year, month, day, lat, lon, grid, orbit, original_projection, ('_%s' % (new_projection) if new_projection is not None else '')),
                'satellite': 'sentinel-2%s' % (satellite.lower()),
                'lat': lat,
                'lon': lon,
                'orbit': orbit,
                'original_projection': original_projection,
                'new_projection': original_projection       # Not a typo, this happens with Rockall
            }

            if new_projection is not None:
                output[year][month][day][grid]['new_projection']: new_projection

        if file_type is 'vmsk_sharp_rad_srefdem_stdsref':
            output[year][month][day][grid]['product'] = {
                'data': matched,
                'size': sizeof_fmt(remote.size)
            }
        else:
            output[year][month][day][grid][file_type] = {
                'data': matched,
                'size': sizeof_fmt(remote.size)
            }
    else:
        if not grid in output:
            output[grid] = {}

        output[grid]['%s%s%s' % (year, month, day)] = {
            'name': 'S2%s_%s%s%s_lat%slon%s_T%s_ORB%s_%s%s' % (satellite, year, month, day, lat, lon, grid, orbit, original_projection, ('_%s' % (new_projection) if new_projection is not None else '')),
            'satellite': 'sentinel-2%s' % (satellite.lower()),
            'lat': lat,
            'lon': lon,
            'orbit': orbit,
            'original_projection': original_projection,
            'new_projection': original_projection       # Not a typo, this happens with Rockall
        }

        if new_projection is not None:
            output[grid]['%s%s%s' % (year, month, day)]['new_projection']: new_projection

        if file_type is 'vmsk_sharp_rad_srefdem_stdsref':
            output[grid]['%s%s%s' % (year, month, day)]['product'] = {
                'data': matched,
                'size': sizeof_fmt(remote.size)
            }
        else:
            output[grid]['%s%s%s' % (year, month, day)][file_type] = {
                'data': matched,
                'size': sizeof_fmt(remote.size)
            }
    
    return output

def create_thumbnail(s3client, bucket, product, tempdir):
    s3client.download_file(bucket, product, os.path.join(tempdir, os.path.basename(product)))

    p = subprocess.Popen('gdal_translate -b 3 -b 2 -b 1 -ot Byte -of JPEG -outsize 5%% 5%% %s %s' % (os.path.join(tempdir, os.path.basename(product)), os.path.join(tempdir, os.path.basename(product).replace('_vmsk_sharp_rad_srefdem_stdsref.tif', '_thumbnail.jpg'))), shell=True)
    (output, err) = p.communicate()
    if output is not None:
        self.logger.debug(output)
    if err is not None:
        raise RuntimeError(err)
    
    s3client.upload_file(os.path.join(tempdir, os.path.basename(product).replace('_vmsk_sharp_rad_srefdem_stdsref.tif', '_thumbnail.jpg')), bucket, product.replace('_vmsk_sharp_rad_srefdem_stdsref.tif', '_thumbnail.jpg'))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Runs through S3 directory looking for S2 ARD images and generates a html structure to view them in')
    parser.add_argument('-b', '--bucket', type=str, required=True, help='S3 Bucket to look in')
    parser.add_argument('-p', '--profile', type=str, required=True, help='Profile to use when connecting to S3')
    parser.add_argument('-i', '--input', type=str, required=False, default='', help='Folder prefix to use when scanning S3 bucket')
    parser.add_argument('-t', '--tempdir', type=str, required=False, default='./temp', help='Local temporary directory [Default: ./temp]')

    args = parser.parse_args()

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    logger = logging.getLogger('s2init')
    logger.setLevel(logging.DEBUG)

    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    ch.setFormatter(formatter)

    # fh = logging.FileHandler('s2init-%s.log' % time.strftime('%y%m%d-%H%M%S'))
    # fh.setFormatter(formatter)
    # fh.setLevel(logging.DEBUG)

    logger.addHandler(ch)
    # logger.addHandler(fh)    

    session = boto3.Session(profile_name=args.profile)
    s3 = session.resource('s3')
    s3c = session.client('s3')
    bucket = s3.Bucket(args.bucket)    

    output_date = {}
    output_grid = {}

    regex = re.compile('Sentinel2([AB])\_((20[0-9]{2})([0-9]{2})([0-9]{2}))\/SEN2\_[0-9]{8}\_lat([0-9]{2,4})lon([0-9]{2,4})\_T([0-9]{2}[A-Z]{3})\_ORB([0-9]{3})\_(utm[0-9]{2}n)(\_osgb)?\_(clouds|sat|toposhad|valid|vmsk_sharp_rad_srefdem_stdsref|meta|thumbnail)(?!\.xml)')

    logger.info('Scanning bucket (%s) path \'%s\' for files' % (args.bucket, args.input))
    for remote in bucket.objects.filter(Prefix=args.input):
        match = regex.search(remote.key)
        logger.info(remote.key)
        if match:
            satellite = match.group(1)
            full_date = match.group(2)
            year = match.group(3)
            month = match.group(4)
            day = match.group(5)
            lat = match.group(6)
            lon = match.group(7)
            grid = match.group(8)
            orbit = match.group(9)
            original_projection = match.group(10)
            new_projection = match.group(11) # Optional Group
            file_type = match.group(12)

            # do something
            output_date = add_item(output_date, remote.key, satellite, year, month, day, lat, lon, grid, orbit, original_projection, new_projection, file_type, remote, year_slicing=True)
            logger.info(output_date)
            output_grid = add_item(output_grid, remote.key, satellite, year, month, day, lat, lon, grid, orbit, original_projection, new_projection, file_type, remote, year_slicing=False)
            logger.info(output_grid)

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
            index.write('<td width=25%>%s</td>\n' % (year))
            index.write('<td>\n')
            for month in output_date[year]:
                index.write('<a href="%s/%s">%s</a><br/>\n' % (year, month, calendar.month_name[int(month)]))
                if not os.path.exists(os.path.join(date_temp_path, year)):
                    os.makedirs(os.path.join(date_temp_path, year))
                with open(os.path.join(os.path.join(date_temp_path, year), '%s.html' % month), 'w') as month_index:
                    month_index.write('<table>\n')
                    month_index.write('<tr>\n')
                    for day in output_date[year][month]:
                        month_index.write('<td width=10%>%s</td>\n' % (day))
                        for grid in output_date[year][month][day]:
                            # month_index.write('<td><img src="%s" height=100px width=100px></td>\n' % (output_date[year][month][day][grid]['thumbnail']['data']))
                            month_index.write('<td>\n')
                            month_index.write('<h3>%s</h3><br/>\n' % (output_date[year][month][day][grid]['name']))
                            month_index.write('<a href="%s">Data [GeoTIFF] (%s)</a><br/>\n' % (output_date[year][month][day][grid]['product']['data'], output_date[year][month][day][grid]['product']['size']))
                            month_index.write('<a href="%s">Cloudmask [GeoTIFF] (%s)</a><br/>\n' % (output_date[year][month][day][grid]['clouds']['data'], output_date[year][month][day][grid]['clouds']['size']))
                            month_index.write('<a href="%s">Saturated pixel mask [GeoTIFF] (%s)</a><br/>\n' % (output_date[year][month][day][grid]['sat']['data'], output_date[year][month][day][grid]['sat']['size']))
                            month_index.write('<a href="%s">Valid pixel mask [GeoTIFF] (%s)</a><br/>\n' % (output_date[year][month][day][grid]['valid']['data'], output_date[year][month][day][grid]['valid']['size']))
                            month_index.write('<a href="%s">Topographic shadow mask [GeoTIFF] (%s)</a><br/>\n' % (output_date[year][month][day][grid]['toposhad']['data'], output_date[year][month][day][grid]['toposhad']['size']))
                            month_index.write('<a href="%s">Metadata [JSON] (%s)</a>\n' % (output_date[year][month][day][grid]['metadata']['data'], output_date[year][month][day][grid]['metadata']['size']))
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
            with open(os.path.join(os.path.join(date_temp_path, year), 'index.html'), 'w') as grid_index:
                grid_index.write('<h1>%s</h1>' % grid)
                grid_index.write('<table>\n')
                grid_index.write('<tr>\n')
                for date in output_grid[grid]:
                    grid_index.write('<td width=10%>%s</td>\n' % (day))
                    # grid_index.write('<td><img src="%s" height=100px width=100px></td>\n' % (output_grid[grid][date]['thumbnail']['data']))
                    grid_index.write('<td>\n')
                    grid_index.write('<h3>%s</h3><br/>\n' % (output_grid[grid][date]['name']))
                    grid_index.write('<a href="%s">Data [GeoTIFF] (%s)</a><br/>\n' % (output_grid[grid][date]['product']['data'], output_grid[grid][date]['product']['size']))
                    grid_index.write('<a href="%s">Cloudmask [GeoTIFF] (%s)</a><br/>\n' % (output_grid[grid][date]['clouds']['data'], output_grid[grid][date]['clouds']['size']))
                    grid_index.write('<a href="%s">Saturated pixel mask [GeoTIFF] (%s)</a><br/>\n' % (output_grid[grid][date]['sat']['data'], output_grid[grid][date]['sat']['size']))
                    grid_index.write('<a href="%s">Valid pixel mask [GeoTIFF] (%s)</a><br/>\n' % (output_grid[grid][date]['valid']['data'], output_grid[grid][date]['valid']['size']))
                    grid_index.write('<a href="%s">Topographic shadow mask [GeoTIFF] (%s)</a><br/>\n' % (output_grid[grid][date]['toposhad']['data'], output_grid[grid][date]['toposhad']['size']))
                    grid_index.write('<a href="%s">Metadata [JSON] (%s)</a>\n' % (output_grid[grid][date]['metadata']['data'], output_grid[grid][date]['metadata']['size']))
                    grid_index.write('</td>\n')
                grid_index.write('</tr>\n')
                grid_index.write('</table>\n')

