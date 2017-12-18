

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

    products_by_date = {}
    products_by_grid = {}

    for p in products:
        name = p
        attrs = products[p]['attrs']
        files = products[p]['files']
        print(name)
        print(attrs)
        print(files)
        print('\n')
        #add_by_date(products_by_date, name, attrs, files)
        add_by_grid(products_by_grid, name, attrs, files)

        # with open(os.path.join('.', args.outdir, 'products_by_date.json'), 'w') as f:
        #     json.dump(products_by_date, f, indent=4)    
        with open(os.path.join('.', args.outdir, 'products_by_grid.json'), 'w') as f:
            json.dump(products_by_grid, f, indent=4)    


def add_by_date(output, name, attrs, files):
    # make a data structure like
    # output[year][month][day][grid][product]
    year = attrs['year']
    month = attrs['month']
    day = attrs['day']
    grid = attrs['grid']
    if not year in output:
        output[year] = {}
    if not month in output[year]:
        output[year][month] = {}
    if not day in output[year][month]:
        output[year][month][day] = {}
    if not grid in output[year][month][day]:
        output[year][month][day][grid] = {
            'name': name,
            'attrs': attrs,
            'files': files,
        }

def add_by_grid(output, name, attrs, files):
    # make a data structure like
    # output[grid]
    grid = attrs['grid']
    if not grid in output:
        output[grid] = []
    output[grid].append({
        'name': name,
        'attrs': attrs,
        'files': files,
    })

main()
