#!/bin/bash

NumEvents=$1
#Input SinglePi0E10_cfi_GEN_DIGI2RAW.root

#ulimit -n 4096
export CMS_VERSION=CMSSW_5_3_11
export SCRAM_ARCH=slc5_amd64_gcc462

rm -rf final_data
mkdir final_data
cd final_data
mv ../SinglePi0E10_cfi_GEN_DIGI2RAW.root ./

. /cvmfs/cms.cern.ch/cmsset_default.sh
scramv1 project CMSSW ${CMS_VERSION}
cd ${CMS_VERSION}
eval `scram runtime -sh`
cd ..

cmsDriver.py SinglePi0E10_cfi_GEN --datatier GEN-SIM-RECO,DQM --conditions auto:startup -s RAW2DIGI,L1Reco,RECO,VALIDATION,DQM --eventcontent RECOSIM,DQM -n $NumEvents
#Output SinglePi0E10_cfi_GEN_RAW2DIGI_L1Reco_RECO_VALIDATION_DQM.py
#Output SinglePi0E10_cfi_GEN_RAW2DIGI_L1Reco_RECO_VALIDATION_DQM.root
#Output SinglePi0E10_cfi_GEN_RAW2DIGI_L1Reco_RECO_VALIDATION_DQM_inDQM.root
