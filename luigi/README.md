# Python environment setup
Configure the development environment as follows
```
## Install required apt packages
sudo apt-get install build-essential
sudo apt-get install libcurl4-openssl-dev 

###TODO - Required only for workflows that connect to postgres db's though psycop2 
sudo apt-get install libpq-dev

## Create a python virtual environment
virtualenv luigi_venv - p python2

## Activate the virtual environment
source luigi_venv/bin/activate

## Install python depenancies
pip install -r requirements.txt
```
## Update Python Environment
After adding or updating PIP manged libraires refresh the requirement file.
```
rm requirements.txt
pip freeze > requirements.txt
```

# Before running the scripts
Sorce the environment
```
source luigi_venv/bin/activate
```

