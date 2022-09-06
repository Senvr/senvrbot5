#!/bin/bash
set -e
sudo apt update
sudo apt install python3 python3-pip python3-venv -y
python3 -m . senvrbot
source .bin/activate
python3 --version
python3 -m pip install -r requirements.txt
deactivate
exit 0