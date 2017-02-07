import json
import time

def runProcess():
    print "Doing some clever stuff"
    time.sleep(10)

    # raise Exception "foo bar"
    
    with open('/mnt/state/job.json') as jobSpecFile, open('/mnt/state/output.txt', 'w') as output:    
        jobSpec = json.load(jobSpecFile)
        output.write("More update \n")
        output.write("this is an image update test \n")
        output.write(jobSpec["outputText \n"])

if __name__ == '__main__':
    runProcess()