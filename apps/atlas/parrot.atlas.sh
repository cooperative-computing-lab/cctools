#!/bin/sh

export PARROT_ALLOW_SWITCHING_CVMFS_REPOSITORIES="yes"
export HTTP_PROXY=http://cache01.hep.wisc.edu:3128
export PARROT_CVMFS_REPO='*.cern.ch:pubkey=<BUILTIN-cern.ch.pub>,url=http://cvmfs-stratum-one.cern.ch/opt/*;http://cernvmfs.gridpp.rl.ac.uk/opt/*;http://cvmfs.racf.bnl.gov/opt/* atlas-nightlies.cern.ch:url=http://cvmfs-atlas-nightlies.cern.ch/cvmfs/atlas-nightlies.cern.ch,pubkey=<BUILTIN-cern.ch.pub>'

parrot_run ./atlas.sh
