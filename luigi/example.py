import luigi
from luigi.util import requires
from luigi.util import inherits

class HelloWorld(luigi.Task):
    
    name = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget('helloworld.txt')
    def run(self):
        with self.output().open('w') as outfile:
            outfile.write('Hello World!\n')

@requires(HelloWorld)
class NameSubstituter(luigi.Task):

    # def requires(self):
    #     return HelloWorld()

    def output(self):
        return luigi.LocalTarget(self.input().path + '.name_' + self.name)

    def run(self):
        with self.input().open() as infile, self.output().open('w') as outfile:
            text = infile.read()
            text = text.replace('World', self.name)
            outfile.write(text)

if __name__ == '__main__':
    luigi.run()