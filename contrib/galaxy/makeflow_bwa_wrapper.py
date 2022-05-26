
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
parser.add_option('', '--output_dblog', dest="dblog", type="string")
parser.add_option('', '--output_err', dest="outerr", type="string")

parser.add_option('', '--pwfile', dest="pwfile", type="string")

parser.add_option('', '--user_id', dest="uid", type="string")
parser.add_option('', '--user_job', dest="ujob", type="string")

(options, args) = parser.parse_args()

# SETUP ENVIRONMENT VARIABLES

cur_dir = os.getcwd()
job_num = os.path.basename(cur_dir);

cctools_dir = options.cctools

makeflow='Makeflow'
wq_project_name="galaxy_bwa_"+options.uid+"_"+job_num
wq_password=options.pwfile

output_sam = "output_SAM"

makeflow_log = "makeflow_log"
wq_log = "wq_log"
debug_log = "debug_log"
output_err = "output_err"

# CREATE TMP AND MOVE FILES IN

if options.ref:
	os.symlink(options.ref, "./reference.fa")
else:
	print "No reference provided"
	sys.exit(1)

inputs = "--ref reference.fa "

os.symlink(options.fastq, "./fastq.fq")
inputs += "--fastq fastq.fq "

if options.rfastq:
	os.symlink(options.rfastq, "./rfastq.fq")
	inputs += "--rfastq rfastq.fq "

os.system("makeflow_bwa --algoalign {0} {1} --makeflow {2} --output_SAM {3} {4}".format(
			"bwa_backtrack", inputs, makeflow, output_sam, ' '.join(args)))

os.system("makeflow {0} -T wq -N {1} -J 50 -p 0 -l {2} -L {3} -dall -o {4} --password {5} >&1 2>&1".format(
			makeflow, wq_project_name, makeflow_log, wq_log, debug_log, options.pwfile))

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
if options.rfastq:
	os.remove("./rfastq.fq")
