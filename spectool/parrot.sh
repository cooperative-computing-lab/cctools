#!/bin/sh

export HTTP_PROXY=http://cache01.hep.wisc.edu:3128
export CMS_VERSION=CMSSW_5_3_11
export SCRAM_ARCH=slc5_amd64_gcc462

#SITECONF can be put into the cache directory
parrot_run -M /cvmfs/cms.cern.ch/SITECONF/local=`pwd`/SITECONF/local ./task.sh

# vim: set noexpandtab tabstop=4:
