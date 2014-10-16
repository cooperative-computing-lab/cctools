#!/bin/sh
rm -rf ${CMS_VERSION}
mkdir ${CMS_VERSION}
cd ${CMS_VERSION}

. /cvmfs/cms.cern.ch/cmsset_default.sh
scramv1 project CMSSW ${CMS_VERSION}
cd ${CMS_VERSION}
eval `scram runtime -sh`
cd ..

#add your analysis code here

