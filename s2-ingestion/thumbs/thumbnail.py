
import argparse
import os
import subprocess

def parse_command_line_args():
    p = argparse.ArgumentParser()
    p.add_argument('-i', '--input', type=str, required=True, help='Input file path')
    return p.parse_args()

def main():
    args = parse_command_line_args()
    create_single_thumbnail(args.input)

def create_single_thumbnail(input_path):
    product_ending = '_vmsk_sharp_rad_srefdem_stdsref.tif'
    assert product_ending not in input_path
    output_path = input_path.replace(product_ending, '_thumbnail.jpg')
    shell_command = 'gdal_translate -b 3 -b 2 -b 1 -ot Byte -of JPEG -outsize 5%% 5%% %s %s' % (input_path, output_path)
    p = subprocess.Popen(shell_command, shell=True)
    (output, err) = p.communicate()
    if output is not None:
        self.log.debug(output)
    if err is not None:
        raise RuntimeError(err)

if __name__ == '__main__':
    main()
