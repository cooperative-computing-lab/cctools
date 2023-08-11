#!/bin/bash

tar -xzf env.tar.gz
source bin/activate
python resnet.py -b 64 -r $3 -d $2 -e 30 -s 10 -o $1
