#!/usr/bin/env python
#
#Copyright (C) 2022 The University of Notre Dame
#This software is distributed under the GNU General Public License.
#See the file COPYING for details.
#
# This program implements a way to organize and manage a large number of
# concurrently running GATK instances
# Author: Nick Hazekamp
# Date: 09/03/2013

import optparse, os, sys, tempfile, shutil, stat

class PassThroughParser(optparse.OptionParser):
    def _process_args(self, largs, rargs, values):
        while rargs:
            try:
                optparse.OptionParser._process_args(self,largs,rargs,values)
            except (optparse.BadOptionError,optparse.AmbiguousOptionError), e:
                largs.append(e.opt_str)

#Parse Command Line
parser = PassThroughParser()
parser.add_option('-T',dest='type',type="string")
parser.add_option('--input_file',dest='input',type="string")
parser.add_option('--reference_sequence',dest='ref',type="string")

parser.add_option('--log_to_file',dest='log',type="string")
parser.add_option('--out',dest='output',type="string")

parser.add_option('--mf_log',dest='mflog',type="string",help="Makeflow Log Location")
parser.add_option('--output_dblog',dest='dblog',type="string",help="Makeflow Debug Log Location")
parser.add_option('--wq_log',dest='wqlog',type="string",help="Work Queue Log Location")

parser.add_option('--pwfile',dest='pwfile',type='string')

parser.add_option('--user_id',dest='uid',type='string')
parser.add_option('--user_job',dest='ujob',type='string')

(options, args) = parser.parse_args()

# SETUP ENVIRONMENT VARIABLES

cur_dir = os.getcwd()
job_num = os.path.basename(cur_dir)

cctools_dir = options.cctools

makeflow='Makeflow'
wq_project_name="galaxy_gatk_"+options.uid+"_"+job_num
wq_password=options.pwfile

output_vcf = "output_VCF"
output_log = "output_log"

makeflow_log = "makeflow_log"
makeflow_graph = "makeflow_graph.eps"

wq_log = "wq_log"
wq_graph = "wq_graph.eps"

debug_log = "debug_log"
output_err = "output_err"

# MOVE FILES TO ENV

os.symlink(options.ref, cur_dir+"/reference.fa")

inputs = "--reference_sequence reference.fa --reference_index reference.fa.fai --reference_dict reference.dict "

os.symlink(options.input, cur_dir+"/cur_bam.bam")
inputs += "--input_file cur_bam.bam "

os.system("makeflow_gatk -T {0} {1} --makeflow {2} --out {3} {4} {5}".format(
			options.type, inputs, makeflow, output_vcf, ' '.join(args), debug_log))

os.system("makeflow -T wq -N {0} -p 0 -l {1} -L {2} -d all -o {3} --password {4} &> {5}".format(
			wq_project_name, makeflow_log, wq_log, debug_log, options.pwfile, debug_log)

if options.dblog:
	shutil.copyfile(debug_log, options.dblog)
if options.mflog:
	shutil.copyfile(makeflow_log, options.mflog)
if options.wqlog:
	shutil.copyfile(wq_log, options.wqlog)
shutil.copyfile(output_vcf, options.output)

os.system(cctools_dir+'/bin/makeflow -c')
os.remove("./reference.*")
os.remove("./cur_bam.bam")
os.remove("./samtools")
os.remove("./GenomeAnalysisTK.jar")
os.remove("./picard.jar")
os.remove("./jre")
