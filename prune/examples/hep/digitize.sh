#!/bin/bash

NumEvents=$1
#Input SinglePi0E10_cfi_GEN_SIM.root

#ulimit -n 4096
export CMS_VERSION=CMSSW_5_3_11
export SCRAM_ARCH=slc5_amd64_gcc462

rm -rf final_data
mkdir final_data
cd final_data
mv ../SinglePi0E10_cfi_GEN_SIM.root ./

. /cvmfs/cms.cern.ch/cmsset_default.sh
scramv1 project CMSSW ${CMS_VERSION}
cd ${CMS_VERSION}
eval `scram runtime -sh`
cd ..

cmsDriver.py SinglePi0E10_cfi_GEN  --datatier GEN-SIM-DIGI-RAW-HLTDEBUG --conditions auto:startup -s DIGI,L1,DIGI2RAW,HLT:@relval,RAW2DIGI,L1Reco --eventcontent FEVTDEBUGHLT -n $NumEvents
#Output SinglePi0E10_cfi_GEN_DIGI_L1_DIGI2RAW_HLT_RAW2DIGI_L1Reco.root
