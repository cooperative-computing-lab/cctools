#!/usr/bin/env python

# Copyright (c) 2010- The University of Notre Dame.
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

from prune import client
import os

prune = client.Connect(base_dir = os.environ["HOME"] + '/.prune') #Prune data is stored in base_dir

###### Create common HEP environment ######
E1 = prune.envi_add(engine='umbrella',
                    spec='cms.umbrella',
                    sandbox_mode='parrot',
                    log='umbrella.log',
                    cms_siteconf='SITECONF.tar.gz',
                    cvmfs_http_proxy='http://eddie.crc.nd.edu:3128',
                    http_proxy='http://eddie.crc.nd.edu:3128' )

event_count = 10

###### Simulation stage ######
D1 = prune.file_add('simulate.sh')
D2, = prune.task_add(returns=['SinglePi0E10_cfi_GEN_SIM.root'],
                     env=E1,
                     cmd='chmod 755 simulate.sh; ./simulate.sh %i ' % (event_count),
                     args=[D1],
                     params=['simulate.sh'])

###### Digitization stage ######
D3 = prune.file_add( 'digitize.sh' )
D4, = prune.task_add(returns=['SinglePi0E10_cfi_GEN_DIGI_L1_DIGI2RAW_HLT_RAW2DIGI_L1Reco.root'],
                     env=E1,
                     cmd='chmod 755 digitize.sh; ./digitize.sh %i' % (event_count),
                     args=[D3,D2],
                     params=['digitize.sh','SinglePi0E10_cfi_GEN_SIM.root'])

###### Reconstruction stage ######
D5 = prune.file_add('reconstruct.sh')
D6, D7, D8 = prune.task_add(returns=['SinglePi0E10_cfi_GEN_RAW2DIGI_L1Reco_RECO_VALIDATION_DQM.py',
                                     'SinglePi0E10_cfi_GEN_RAW2DIGI_L1Reco_RECO_VALIDATION_DQM.root',
                                     'SinglePi0E10_cfi_GEN_RAW2DIGI_L1Reco_RECO_VALIDATION_DQM_inDQM.root'],
                            env=E1,
                            cmd='chmod 755 reconstruct.sh; ./reconstruct.sh %i' % (event_count),
                            args=[D5,D4],
                            params=['reconstruct.sh','SinglePi0E10_cfi_GEN_DIGI2RAW.root'])

###### Execute the workflow ######
prune.execute( worker_type='local', cores=8 )

###### Export final data ######
prune.export( D7, 'hep.result.root' )


###### Export publishable workflow ######
prune.export( D7, 'hep.prune', lineage=3 )
