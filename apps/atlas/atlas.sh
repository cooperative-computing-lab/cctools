#! /bin/sh

export ATLAS_LOCAL_ROOT_BASE=/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase

echo "atlasLocalSetup"
. ${ATLAS_LOCAL_ROOT_BASE}/user/atlasLocalSetup.sh

echo "cleaning up workspace"
rm -rf athena/17.2.4
mkdir -p athena/17.2.4

echo "asetup"
asetup 17.2.4,here

# In production, PhysicsAnalysis would be checked out from this code repository.
# cmt co -r UserAnalysis-00-15-06 PhysicsAnalysis/AnalysisCommon/UserAnalysis

# Since we do not have access to that repository, we use this tarball instead:
echo "unpacking analysis data"
tar -xzf PhysicsAnalysis.tar.gz
cd PhysicsAnalysis/AnalysisCommon/UserAnalysis/run

JOBOPTSEARCHPATH=/cvmfs/atlas.cern.ch/repo/sw/Generators/MC12JobOptions/latest/common:$JOBOPTSEARCHPATH
JOBOPTSEARCHPATH=/cvmfs/atlas.cern.ch/repo/sw/Generators/MC12JobOptions/latest/share/DSID147xxx:$JOBOPTSEARCHPATH

echo "running simulation"
Generate_trf.py ecmEnergy=8000. runNumber=105144 firstEvent=1 maxEvents=10 randomSeed=1324354657 jobConfig=MC12.147816.Pythia8_AU2CTEQ6L1_Zee.py outputEVNTFile=pythia.EVNT.pool.root

