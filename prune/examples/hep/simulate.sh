#!/bin/bash

#PRUNE_INPUTS TEXT
#PRUNE_OUTPUT SinglePi0E10_cfi_GEN_SIM.root

#ulimit -n 4096
export CMS_VERSION=CMSSW_5_3_11
export SCRAM_ARCH=slc5_amd64_gcc462

#rm -rf cmsjob
#mkdir cmsjob
#cd cmsjob

. /cvmfs/cms.cern.ch/cmsset_default.sh
scramv1 project CMSSW ${CMS_VERSION}
cd ${CMS_VERSION}
eval `scram runtime -sh`
cd ..

NumEvents=$1
cmsDriver.py SinglePi0E10_cfi --conditions auto:startup -s GEN,SIM --datatier GEN-SIM -n $NumEvents --relval 25000,100 --eventcontent RAWSIM
