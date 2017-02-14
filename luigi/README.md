# Python environment setup
Configure the development environment as follows
```
## Install required apt packages
sudo apt-get install build-essential
sudo apt-get install libcurl4-openssl-dev 
sudo apt-get install virtualenv
sudo apt-get install libgeos-dev
sudo apt-get install python-dev
sudo apt-get install gdal-bin
```

###TODO - Required only for workflows that connect to postgres db's though psycop2 
sudo apt-get install libpq-dev

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

