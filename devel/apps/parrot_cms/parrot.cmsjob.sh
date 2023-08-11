#!/bin/sh

export HTTP_PROXY=http://cache01.hep.wisc.edu:3128

parrot_run -M /cvmfs/cms.cern.ch/SITECONF/local=`pwd`/SITECONF/local ./cmsjob.sh

# vim: set noexpandtab tabstop=4:
