# Python environment setup
Configure the development environment as follows

## Add the ustable gdal repo for ubuntu
sudo add-apt-repository ppa:ubuntugis/ubuntugis-unstable -y
sudo apt-get update

## Install required apt packages
sudo add-apt-repository ppa:ubuntugis/ubuntugis-unstable
sudo apt-get update
sudo apt-get install build-essential libcurl4-openssl-dev virtualenv libgeos-dev python-dev gdal-bin libpq-dev libgdal-dev

## AWS client install
The aws client is used to manage default credentials for luigi / docker.
Per process credentials are useually managed witin the process configuration, ie config files

```
pip install --user awscli
```
Determine the shell config file
Bash – .bash_profile, .profile, or .bash_login.
Zsh – .zshrc
Tcsh – .tcshrc, .cshrc or .login.
Add an export command to profile script.

```
export PATH=~/.local/bin:$PATH
```
log out and back in again 
```
aws configure
```
Input your access key and secret

## Create a python virtual environment
```
virtualenv luigi_venv -p python2
```
Activate the virtual environment
```
source luigi_venv/bin/activate
```
Install python depenancies
```
export CPLUS_INCLUDE_PATH=/usr/include/gdal
export C_INCLUDE_PATH=/usr/include/gdal
pip install -r requirements.txt
```
## Update Python Environment
After adding or updating PIP manged libraires refresh the requirement file.
```
rm requirements.txt
pip freeze > requirements.txt
```





# Docker host setup
install docker as here:
https://docs.docker.com/engine/installation/linux/ubuntu/

## Configure to run as non root user
```
sudo groupadd docker  
sudo gpasswd -a ${USER} docker 
sudo service docker restart 
```
log out and back in again



# Before running the scripts
Sorce the environment
```
source luigi_venv/bin/activate
```
# Luigi daemon commands

Run luigi d
luigid --background --pidfile /var/run/luigi.pid --logdir /var/log/luigi --state-path /var/lib/luigi/luigi-state.pickle

## luigi esa downloader

Delete aws state
aws s3 rm s3://jncc-data/luigi/sentinel/esa_downloader/2017-02-02 --recursive
aws s3 rm s3://jncc-data/luigi/sentinel/esa_downloader/2017-02-03 --recursive

Run seed
PYTHONPATH='.' luigi --module workflow DownloadAvailableProducts  --runDate=2017-02-02 --seedDate=2017-01-01  --debug

Run 2
PYTHONPATH='.' luigi --module workflow DownloadAvailableProducts  --runDate=2017-02-03 --debug

## luigi s2 ard process

# Demo processing environment

/mnt/data

# Demo workflow
start 2 terminals to see container fire up

PYTHONPATH='.' luigi --module workflow CreateArdProduct --text "this is mega fun" --runDate=2017-02-03

docker ps 


