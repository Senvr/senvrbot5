#!/bin/bash -xe
sudo apt-get update
sudo apt-get install python3 python3-pip python3-venv -y
python3 -m venv .
source ./bin/activate
python3 --version
python3 -m pip install --upgrade pip setuptools wheel
python3 -m pip install -r requirements.txt
deactivate
exit 0