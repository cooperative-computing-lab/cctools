#!/bin/sh

export CMS_VERSION=CMSSW_5_2_5
export SCRAM_ARCH=slc5_amd64_gcc462

rm -rf sim_job
mkdir sim_job
cd sim_job

. /cvmfs/cms.cern.ch/cmsset_default.sh
scramv1 project -f CMSSW ${CMS_VERSION}
cd ${CMS_VERSION}
eval `scram runtime -sh`
cd ..
cmsDriver.py Hadronizer_MgmMatchTuneZ2star_8TeV_madgraph_cff.py -s GEN \
--eventcontent=RAWSIM --datatier GEN -n -1 \
--filein=file:/tmp/final_events_2381.lhe \
--filetype=LHE --conditions=START52_V9::All

#--filein=file:/afs/crc.nd.edu/user/a/awoodard/Public/for_haiyan/many_events.lhe \
