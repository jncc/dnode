

import argparse
import calendar
import json
import logging
import os
import re
import subprocess
import sys
import time
from types import SimpleNamespace

def parse_command_line_args():
    p = argparse.ArgumentParser()
    p.add_argument('-i', '--indir', type=str, required=False, default='output', help='Directory for previous step output')
    p.add_argument('-o', '--outdir', type=str, required=False, default='output', help='Local output directory [Default: ./output]')
    return p.parse_args()

def main():

    args = parse_command_line_args()

    by_date_input_path = os.path.join('.', args.indir, 'by_date.json')
    by_date_output_dir = os.path.join('.', args.outdir, 'by_date_html')

    with open(by_date_input_path) as f:    
        by_date_data = json.load(f)

    make_html_by_date(by_date_data, by_date_output_dir)

    # by_grid_input_path = os.path.join('.', args.indir, 'by_grid.json')
    # by_grid_output_dir = os.path.join('.', args.outdir, 'by_grid_html')
    # with open(by_grid_input_path) as f:    
    #     by_grid = json.load(f)


def make_html_by_date(data, outdir):

    if not os.path.exists(outdir):
        os.makedirs(outdir)
    
    # Built output dictionary
    with open(os.path.join(outdir, 'index.html'), 'w') as index:
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
        
        for year in data:
            index.write('<tr>\n')
            index.write('<td width=25%%>%s</td>\n' % (year))
            index.write('<td>\n')
            for month in data[year]:
                index.write('<a href="%s/%s.html">%s</a><br/>\n' % (year, month, calendar.month_name[int(month)]))
                year_dir_path = os.path.join('.', outdir, year)
                if not os.path.exists(year_dir_path):
                    os.makedirs(year_dir_path)
                with open(os.path.join(year_dir_path, '%s.html' % month), 'w') as month_index:
                    month_index.write('<table>\n')
                    for day in data[year][month]:
                        for grid in data[year][month][day]:
                            month_index.write('<tr>\n')
                            # month_index.write('<td><img src="%s" height=100px width=100px></td>\n' % (data[year][month][day][grid]['thumbnail']['data']))
                            month_index.write('<td>\n')
                            month_index.write('<h3>%s</h3>\n' % (data[year][month][day][grid]['name']))
                            month_index.write('<a href="%s">Data [GeoTIFF] (%s)</a><br/>\n' % (data[year][month][day][grid]['product']['data'], data[year][month][day][grid]['product']['size']))
                            month_index.write('<a href="%s">Cloudmask [GeoTIFF] (%s)</a><br/>\n' % (data[year][month][day][grid]['clouds']['data'], data[year][month][day][grid]['clouds']['size']))
                            month_index.write('<a href="%s">Saturated pixel mask [GeoTIFF] (%s)</a><br/>\n' % (data[year][month][day][grid]['sat']['data'], data[year][month][day][grid]['sat']['size']))
                            month_index.write('<a href="%s">Valid pixel mask [GeoTIFF] (%s)</a><br/>\n' % (data[year][month][day][grid]['valid']['data'], data[year][month][day][grid]['valid']['size']))
                            month_index.write('<a href="%s">Topographic shadow mask [GeoTIFF] (%s)</a><br/>\n' % (data[year][month][day][grid]['toposhad']['data'], data[year][month][day][grid]['toposhad']['size']))
                            month_index.write('<a href="%s">Metadata [JSON] (%s)</a>\n' % (data[year][month][day][grid]['meta']['data'], data[year][month][day][grid]['meta']['size']))
                            month_index.write('</td>\n')
                            month_index.write('</tr>\n')
                    month_index.write('</table>\n')
            index.write('</td>\n')
            index.write('</tr>\n')
        index.write('</tbody></table></body></html>\n')

main()
