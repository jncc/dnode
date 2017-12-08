


def create_thumbnail(s3client, bucket, product, outdir):
    s3client.download_file(bucket, product, os.path.join(outdir, os.path.basename(product)))

    p = subprocess.Popen('gdal_translate -b 3 -b 2 -b 1 -ot Byte -of JPEG -outsize 5%% 5%% %s %s' % (os.path.join(outdir, os.path.basename(product)), os.path.join(outdir, os.path.basename(product).replace('_vmsk_sharp_rad_srefdem_stdsref.tif', '_thumbnail.jpg'))), shell=True)
    (output, err) = p.communicate()
    if output is not None:
        self.log.debug(output)
    if err is not None:
        raise RuntimeError(err)
    
    s3client.upload_file(os.path.join(outdir, os.path.basename(product).replace('_vmsk_sharp_rad_srefdem_stdsref.tif', '_thumbnail.jpg')), bucket, product.replace('_vmsk_sharp_rad_srefdem_stdsref.tif', '_thumbnail.jpg'))
