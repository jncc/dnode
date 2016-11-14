import luigi
from luigi.s3 import S3Target


class MyS3File(luigi.ExternalTask):

    def output(self):
        return luigi.S3Target('s3://s3-eu-west-1.amazonaws.com/jncc-data/example/hello.txt')
# s3-eu-west-1.amazonaws.com/eodip/sentinel/S1A_IW_GRDH_1SDV_20151119T175834_20151119T175859_008677_00C566_A3A0.zip
# https://s3-eu-west-1.amazonaws.com/jncc-data/example/hello.txt

class ProcessS3File(luigi.Task):

    def requires(self):
        return MyS3File()

    def output(self):
        return luigi.S3Target(self.input().path + '.name_' + self.name)

    def run(self):
        with self.input().open() as infile, self.output().open('w') as outfile:
            text = infile.read()
            text = text.replace('World', self.name)
            outfile.write(text)

