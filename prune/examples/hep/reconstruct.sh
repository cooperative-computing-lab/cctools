#!/bin/bash

#PRUNE_INPUTS Text File
#PRUNE_OUTPUT SinglePi0E10_cfi_GEN_RAW2DIGI_L1Reco_RECO_VALIDATION_DQM.py
#PRUNE_OUTPUT SinglePi0E10_cfi_GEN_RAW2DIGI_L1Reco_RECO_VALIDATION_DQM.root
#PRUNE_OUTPUT SinglePi0E10_cfi_GEN_RAW2DIGI_L1Reco_RECO_VALIDATION_DQM_inDQM.root

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
ln -s $2 SinglePi0E10_cfi_GEN_DIGI2RAW.root
cmsDriver.py SinglePi0E10_cfi_GEN --datatier GEN-SIM-RECO,DQM --conditions auto:startup -s RAW2DIGI,L1Reco,RECO,VALIDATION,DQM --eventcontent RECOSIM,DQM -n $NumEvents
