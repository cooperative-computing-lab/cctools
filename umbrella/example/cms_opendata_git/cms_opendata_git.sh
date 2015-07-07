#!/bin/sh

. ${CMS_DIR}/cmsset_default.sh

#Execute the following command; this command builds the local release area (the directory structure) for CMSSW, and only needs to be run once:
scramv1 project -f CMSSW ${CMS_VERSION}

#Change to the CMSSW_4_2_8/src/ directory:
cd ${CMS_VERSION}/src/

git init
#use environment variable GIT_REPO_1 instead of the hard-coded URL (https://github.com/ayrodrig/OutreachExercise2010.git)
git remote add origin ${GIT_REPO_1}
git fetch origin
git checkout master

#Compile the code:
scram b

#Then, run the following command to create the CMS runtime variables:
eval `scram runtime -sh`

#Move to the run directory: 
cd OutreachExercise2010/DecaysToLeptons/run

#And then run:
python run.py
