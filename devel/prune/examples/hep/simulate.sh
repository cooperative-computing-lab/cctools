#!/bin/bash

NumEvents=$1

#ulimit -n 4096
export CMS_VERSION=CMSSW_5_2_5
export SCRAM_ARCH=slc5_amd64_gcc462

rm -rf final_data
mkdir final_data
cd final_data

. /cvmfs/cms.cern.ch/cmsset_default.sh
scramv1 project CMSSW ${CMS_VERSION}
cd ${CMS_VERSION}
eval `scram runtime -sh`
cd ..

cmsDriver.py SinglePi0E10_cfi --conditions auto:startup -s GEN,SIM --datatier GEN-SIM -n $NumEvents --relval 25000,100 --eventcontent RAWSIM
# Output SinglePi0E10_cfi_GEN_SIM.root
