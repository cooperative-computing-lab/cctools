
# This program implements a distributed version of BWA, using Makeflow and WorkQueue

# Author: Olivia Choudhury
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
parser.add_option('', '--ref', dest="ref", type="string")

parser.add_option('', '--fastq', dest="fastq", type="string")
parser.add_option('', '--rfastq', dest="rfastq", type="string")

parser.add_option('', '--output_SAM', dest="outsam", type="string")

parser.add_option('', '--output_log', dest="outlog", type="string")
parser.add_option('', '--wq_log', dest="wqlog", type="string")

parser.add_option('', '--cctools_install', dest="cctools", type="string")

parser.add_option('', '--output_dblog', dest="dblog", type="string")
parser.add_option('', '--output_err', dest="outerr", type="string")

parser.add_option('', '--tmp_dir', dest="tmp_dir", type="string")
parser.add_option('', '--pwfile', dest="pwfile", type="string")

parser.add_option('', '--user_id', dest="uid", type="string")
parser.add_option('', '--user_job', dest="ujob", type="string")

(options, args) = parser.parse_args()

# SETUP ENVIRONMENT VARIABLES

cur_dir = os.getcwd()
job_num = os.path.basename(cur_dir);

os.environ['TCP_LOW_PORT'] = '9123'
os.environ['TCP_HIGH_PORT'] = '9173'

cctools_dir = options.cctools

makeflow='Makeflow'
wq_project_name="galaxy_bwa_"+options.uid+"_"+job_num
wq_password=options.pwfile

output_sam = "output_SAM"

makeflow_log = "makeflow_log"
makeflow_graph = "makeflow_graph.eps"

wq_log = "wq_log"
wq_graph = "wq_graph.eps"

debug_log = "debug_log"
output_err = "output_err"

# CREATE TMP AND MOVE FILES IN

	
shutil.copyfile(options.pwfile, "./mypwfile")

os.symlink(cctools_dir+"/apps/makeflow_bwa/makeflow_bwa", "./makeflow_bwa")

if options.ref:
	shutil.copyfile(options.ref, "./reference.fa")
else:
	sys.exit(1)

inputs = "--ref reference.fa "

shutil.copyfile(options.fastq, "./fastq.fq")
inputs += "--fastq fastq.fq "

if options.rfastq:
	os.symlink(options.rfastq, "./rfastq.fq")
	inputs += "--rfastq rfastq.fq "

shutil.copyfile("/usr/bin/bwa", "./bwa")
os.chmod("bwa", os.stat("bwa").st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)

os.system('python ./makeflow_bwa --algoalign bwa_backtrack ' + inputs + 
	  '--makeflow ' +  makeflow + ' --output_SAM ' + output_sam +' '+ ' '.join(args))

os.system(cctools_dir+'/bin/makeflow ' 
	  ' -T wq -N ' + wq_project_name + ' -J 50 -p 0 -l ' + makeflow_log + 
	  ' -L '+wq_log+' -d all -o ' + debug_log+" --password mypwfile >&1 2>&1 ")

if options.dblog:
	shutil.copyfile(debug_log, options.dblog)

if options.outlog:
	shutil.copyfile(makeflow_log, options.outlog)

if options.wqlog:
	shutil.copyfile(wq_log, options.wqlog)

shutil.copyfile(output_sam, options.outsam)

os.system(cctools_dir+'/bin/makeflow -c') 
os.remove("./reference.fa")
os.remove("./fastq.fq")
os.remove("./makeflow_bwa")
os.remove("./bwa")
os.remove("./mypwfile")
if options.rfastq:
	os.remove("./rfastq.fq")
