#!/bin/sh

rm -rf cmsjob
mkdir cmsjob
cd cmsjob

. /cvmfs/cms.cern.ch/cmsset_default.sh
scramv1 project CMSSW ${CMS_VERSION}
cd ${CMS_VERSION}
eval `scram runtime -sh`
cd ..
cmsDriver.py TTbar_Tauola_7TeV_cfi --conditions auto:startup -s GEN,SIM --datatier GEN-SIM -n 0 --relval 9000,50 --eventcontent RAWSIM --no_output
