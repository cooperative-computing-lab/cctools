#!/usr/bin/env cctools_python
# CCTOOLS_PYTHON_VERSION 2.7 2.6

# Copyright (c) 2010- The University of Notre Dame.
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

from prune import client
prune = client.Connect() #Connect to SQLite3

###### Create common HEP environment ######
E1 = prune.envi_add( engine='umbrella', spec='cms.umbrella',
        cvmfs_http_proxy='http://cache01.hep.wisc.edu:3128',
        sandbox_mode='parrot', log='umbrella.log' )
#The value http://cache01.hep.wisc.edu:3128 is hard coded as the HTTP_PROXY
#In future versions of Prune, this value will be configurable.

event_count = 10

###### Simulation stage ######
D1 = prune.file_add( 'simulate.sh' )
D2, = prune.task_add( returns=['SinglePi0E10_cfi_GEN_SIM.root'],
    env=E1, cmd='simulate.sh %i' % (event_count),
    args=[D1], params=['simulate.sh'] )


###### Digitization stage ######
D3 = prune.file_add( 'digitize.sh' )
D4, = prune.task_add( returns=['SinglePi0E10_cfi_GEN_DIGI_L1_DIGI2RAW_HLT_RAW2DIGI_L1Reco.root'],
    env=E1, cmd='digitize.sh %i' % (event_count),
    args=[D3,D2], params=['digitize.sh','SinglePi0E10_cfi_GEN_SIM.root'] )

###### Reconstruction stage ######
D5 = prune.file_add( 'reconstruct.sh' )
D6, D7, D8 = prune.task_add( returns=[
    'SinglePi0E10_cfi_GEN_RAW2DIGI_L1Reco_RECO_VALIDATION_DQM.py',
    'SinglePi0E10_cfi_GEN_RAW2DIGI_L1Reco_RECO_VALIDATION_DQM.root',
    'SinglePi0E10_cfi_GEN_RAW2DIGI_L1Reco_RECO_VALIDATION_DQM_inDQM.root'],
    env=E1, cmd='reconstruct.sh %i' % (event_count),
    args=[D5,D4], params=['reconstruct.sh','SinglePi0E10_cfi_GEN_DIGI2RAW.root'] )

###### Execute the workflow ######
prune.execute( worker_type='work_queue', name='prune_hep_example' )

###### Export final data ######
prune.export( D7, 'hep.result.root' )


###### Export publishable workflow ######
prune.export( D7, 'hep.prune', lineage=3 )
