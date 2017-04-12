# Requires 
docker/sentinel/s2-ard-processor image

# Create workflow
## GenerateJob
- Creates job.json

Outputs: job.json
## ProcessJob
Requires - GenerateJob
- Instatiates docker image
- copies job.json to image
- Triggers proecess
- copies result back
Outputs - output.txt
### Nice to haves 
- Stream logs (stdout, stderr) to local log file

# Run test job

    source ../../luigi_venv/bin/activate
    PYTHONPATH='.' luigi --module workflow CreateArdProduct --text "this is a test" --runDate=2017-01-26 --local-scheduler

