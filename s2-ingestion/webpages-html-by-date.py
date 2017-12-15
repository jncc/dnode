

import argparse
import calendar
import json
import logging
import os
import re
import subprocess
import sys
import time

def parse_command_line_args():
    p = argparse.ArgumentParser()
    p.add_argument('-i', '--input', type=str, required=True, help='Input from previous step output')
    p.add_argument('-o', '--outdir', type=str, required=False, default='output', help='Local output directory [Default: ./output]')
    return p.parse_args()

def main():
    args = parse_command_line_args()

    with open(args.input) as f:
        products = json.load(f)

    print(products.keys())

    make_html_by_date(products, os.path.join('.', args.outdir, 'by_date_html'))

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
        <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/semantic-ui/2.2.13/semantic.min.css"/>
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
                    month_index.write('<html><head><title></title><link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/semantic-ui/2.2.13/semantic.min.css"/></head><body>\n')
                    month_index.write('<table>\n')
                    for day in data[year][month]:
                        for grid in data[year][month][day]:
                            s3_base = 'https://s3-eu-west-1.amazonaws.com/eocoe-sentinel-2/'
                            p = data[year][month][day][grid] # the product at this gridsquare
                            product_file = next((f for f in p['files'] if f['type']=='product'), None)
                            clouds_file = next((f for f in p['files'] if f['type']=='clouds'), None)
                            meta_file = next((f for f in p['files'] if f['type']=='meta'), None)
                            sat_file = next((f for f in p['files'] if f['type']=='sat'), None)
                            toposhad_file = next((f for f in p['files'] if f['type']=='toposhad'), None)
                            valid_file = next((f for f in p['files'] if f['type']=='valid'), None)

                            if product_file is not None:
                                month_index.write('<tr>\n')
                                month_index.write('<td><img src="%s" height=100px width=100px></td>\n' % ('TODO'))
                                month_index.write('<td>\n')
                                month_index.write('<h3>%s</h3>\n' % (p['name']))
                                month_index.write('<a href="%s">Data [GeoTIFF] (%s)</a><br/>\n' % (s3_base + product_file['data'], product_file['size']))
                                if clouds_file is not None:
                                    month_index.write('<a href="%s">Cloudmask [GeoTIFF] (%s)</a><br/>\n' % (s3_base + clouds_file['data'], clouds_file['size']))
                                if sat_file is not None:
                                    month_index.write('<a href="%s">Saturated pixel mask [GeoTIFF] (%s)</a><br/>\n' % (s3_base + sat_file['data'], sat_file['size']))
                                if valid_file is not None:
                                    month_index.write('<a href="%s">Valid pixel mask [GeoTIFF] (%s)</a><br/>\n' % (s3_base + valid_file['data'], valid_file['size']))
                                if toposhad_file is not None:
                                    month_index.write('<a href="%s">Topographic shadow mask [GeoTIFF] (%s)</a><br/>\n' % (s3_base + toposhad_file['data'], toposhad_file['size']))
                                if meta_file is not None:
                                    month_index.write('<a href="%s">Metadata [JSON] (%s)</a>\n' % (s3_base + meta_file['data'], meta_file['size']))
                                month_index.write('<hr/\n')
                                month_index.write('</td>\n')
                                month_index.write('</tr>\n')
                    month_index.write('</table>\n')
                    month_index.write('</body></html>')
            index.write('</td>\n')
            index.write('</tr>\n')
        index.write('</tbody></table></body></html>\n')

main()
