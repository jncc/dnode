#Python environment setup
Configure the development environment as follows
```
# Install required apt packages
sudo apt-get install build-essential
sudo apt-get install libcurl4-openssl-dev 

# Create a python virtual environment
virtualenv luigi_venv

# Activate the virtual environment
source luigi_venv/bin/activate

# Install python depenancies
pip install -r requirements.txt
```
