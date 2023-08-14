#!/bin/sh

export PARROT_ALLOW_SWITCHING_CVMFS_REPOSITORIES="yes"
export HTTP_PROXY=http://cache01.hep.wisc.edu:3128
export PARROT_CVMFS_REPO='*.cern.ch:pubkey=<BUILTIN-cern.ch.pub>,url=http://cvmfs-stratum-one.cern.ch/cvmfs/*.cern.ch;http://cernvmfs.gridpp.rl.ac.uk/cvmfs/*.cern.ch;http://cvmfs.racf.bnl.gov/cvmfs/*.cern.ch atlas-nightlies.cern.ch:url=http://cvmfs-atlas-nightlies.cern.ch/cvmfs/atlas-nightlies.cern.ch,pubkey=<BUILTIN-cern.ch.pub>'

parrot_run ./atlas.sh

# vim: set noexpandtab tabstop=4:
