#! /usr/bin/env python
#
#Copyright (C) 2013- The University of Notre Dame
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

parser.add_option('--cctools_install',dest='cctools',type="string")
parser.add_option('--mf_log',dest='mflog',type="string",help="Makeflow Log Location")
parser.add_option('--output_dblog',dest='dblog',type="string",help="Makeflow Debug Log Location")

parser.add_option('--wq_log',dest='wqlog',type="string",help="Work Queue Log Location")

parser.add_option('--tmp_dir',dest='tmp_dir',type='string')
parser.add_option('--pwfile',dest='pwfile',type='string')

parser.add_option('--user_id',dest='uid',type='string')
parser.add_option('--user_job',dest='ujob',type='string')

parser.add_option('--samtools',dest='samtools',type='string')
parser.add_option('--gatk_jar',dest='gatk_jar',type='string')
parser.add_option('--dict_jar',dest='dict_jar',type='string')
parser.add_option('--java_zip',dest='java_zip',type='string')


(options, args) = parser.parse_args()

# SETUP ENVIRONMENT VARIABLES

cur_dir = os.getcwd()
job_num = os.path.basename(cur_dir)

cctools_dir = options.cctools

os.environ['TCP_LOW_PORT'] = '9123'
os.environ['TCP_HIGH_PORT'] = '9173'

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

os.symlink(cctools_dir+'/apps/makeflow_gatk/makeflow_gatk', cur_dir+"/makeflow_gatk")

shutil.copyfile(options.pwfile, cur_dir+"/mypwfile")

shutil.copyfile(options.ref, cur_dir+"/reference.fa")
#	os.symlink(options.ref, cur_dir+"/reference.fa")

inputs = "--reference_sequence reference.fa --reference_index reference.fa.fai --reference_dict reference.dict "

shutil.copyfile(options.input, cur_dir+"/cur_bam.bam")
#	os.symlink(options.input, cur_dir+"/cur_bam.bam")
inputs += "--input_file cur_bam.bam "

shutil.copyfile(options.samtools, cur_dir+"/samtools")
os.chmod(cur_dir+"/samtools", os.stat(cur_dir+"/samtools").st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)

shutil.copyfile("/usr/bin/java", cur_dir+"/java")
os.chmod(cur_dir+"/java", os.stat(cur_dir+"/java").st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)

shutil.copyfile(options.gatk_jar, cur_dir+"/GenomeAnalysisTK.jar")
shutil.copyfile(options.dict_jar, cur_dir+"/CreateSequenceDictionary.jar")
shutil.copyfile(options.java_zip, cur_dir+"/jre.zip")

os.chdir(cur_dir)

os.system('python makeflow_gatk --verbose -T ' +options.type+ ' ' + inputs + '--progeny_list progeny_file --makeflow ' +  makeflow + ' --out ' + output_vcf +' ' +' '.join(args)+" &> " + debug_log)
	
os.system(cctools_dir+'/bin/makeflow -T wq -N ' + wq_project_name + ' -p 0 -l ' + makeflow_log + ' -L ' + wq_log + ' -d all -o ' + debug_log+" --password mypwfile &> "+debug_log)

if options.dblog:
	shutil.copyfile(debug_log, options.dblog)
if options.mflog:
	shutil.copyfile(makeflow_log, options.mflog)
if options.wqlog:
	shutil.copyfile(wq_log, options.wqlog)
shutil.copyfile(output_vcf, options.output)

os.system(cctools_dir+'/bin/makeflow -c')
#os.remove("./reference.*")
#os.remove("./cur_bam.bam")
#os.remove("./samtools")
#os.remove("./GenomeAnalysisTK.jar")
#os.remove("./CreateSequenceDictionary.jar")
#os.remove("./jre.zip")
#os.remove("./mypwfile")
