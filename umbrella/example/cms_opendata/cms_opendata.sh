#!/bin/sh

#the setting of environment variables is moved into the umbrella specification.

#setting environment variables is not necessary in the analysis code.

rm -rf sim_job
mkdir sim_job
cd sim_job

. ${CMS_DIR}/cmsset_default.sh
#Execute the following command; this command builds the local release area (the directory structure) for CMSSW, and only needs to be run once:
scramv1 project -f CMSSW ${CMS_VERSION}

#Change to the CMSSW_4_2_8/src/ directory:
cd ${CMS_VERSION}/src/

#Then, run the following command to create the CMS runtime variables:
#cmsenv
eval `scram runtime -sh`

#Create a working directory for the demo analyzer, change to that directory and create a "skeleton" for the analyzer:
mkdir Demo
cd Demo
mkedanlzr DemoAnalyzer

#Compile the code:
cd DemoAnalyzer
scram b

#Change the file name in the configuration file demoanalyzer_cfg.py in the DemoAnalyzer directory: i.e. replace file:myfile.root with root://eospublic.cern.ch//eos/opendata/cms/Run2010B/Mu/AOD/Apr21ReReco-v1/0000/00459D48-EB70-E011-AF09-90E6BA19A252.root
#Change the max number of events to 10 (i.e change -1 to 10 in process.maxEvents = cms.untracked.PSet( input = cms.untracked.int32(-1)).
#the problem here is this is an interactive procedure, which by default is not supported by umbrella and parrot-packaging-tool.
cp -f ${CONFIG_FILE} .

#Move two directories back using:
cd ../..

#And then run:
cmsRun Demo/DemoAnalyzer/demoanalyzer_cfg.py

