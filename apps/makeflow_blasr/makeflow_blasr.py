#! /usr/bin/env python

# Copyright (C) 2014- The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.
#
# This program generates makeflows to parallelize the 
# popular blasr program.



import optparse, os, sys, tempfile, shutil, stat, re, string

split_name = "query_r"

class PassThroughParser(optparse.OptionParser):
    def _process_args(self, largs, rargs, values):
        while rargs:
            try:
                optparse.OptionParser._process_args(self,largs,rargs,values)
            except (optparse.BadOptionError,optparse.AmbiguousOptionError), e:
                largs.append(e.opt_str)
                
#Initialize Global Variable
blasr_args = ""

def callback_blasr_args(option, opt_str, value, parser):
	global blasr_args
        blasr_args += opt_str + " " + value + " "

#Parse Command Line
parser = PassThroughParser()
parser.add_option('--ref', dest="reference", type="string", help='The reference input file')
parser.add_option('--query', dest="query", type="string", help='The query input file')
parser.add_option('--output', dest="output", type="string", help='the final output of blasr alignment')
parser.add_option('--makeflow', dest="makeflow", type="string", help='the file name of the makeflow script generated')
parser.add_option('--split_gran', help='Determines number of sequences per search',default=50000,type=int)

(options, args) = parser.parse_args()

def count_splits(split_gran, query):

	num_reads=split_gran
	num_splits = 0
	line_count=0

	FILE = open(query, "r")

	read_count = 0
	num_splits += 1
	for line in FILE:
		if (re.search('^[@]', line) and line_count % 4 ==0):
			if (read_count == num_reads):
				num_splits += 1
				read_count = 0	
			else:
				read_count += 1
		#place all other lines in FASTQ file under same sequence
		line_count += 1
	FILE.close()
	return num_splits
	
	
def write_split_name_reduce(destination):
	split_name = open(destination, 'w')
	try:
		split_name.write("#!/usr/bin/perl\n")
		split_name.write("#\n")
		split_name.write("#Copyright (C) 2013- The University of Notre Dame\n")
		split_name.write("#This software is distributed under the GNU General Public License.\n")
		split_name.write("#See the file COPYING for details.\n")
		split_name.write("#\n")
		split_name.write("#Programmer: Brian Kachmarck\n")
		split_name.write("#Date: 7/28/2009\n")
		split_name.write("#\n")
		split_name.write("#Revised: Nick Hazekamp\n")
		split_name.write("#Date: 12/02/2013\n")
		split_name.write("#\n")
		split_name.write("#Purpose: Split a FASTQ file into smaller files determined by the number of sequences input\n")
		split_name.write("\n")
		split_name.write("use strict; \n")
		split_name.write("\n")
		split_name.write("\n")
		split_name.write("my $numargs = $#ARGV + 1;\n")
		split_name.write("\n")
		split_name.write("my $file = $ARGV[0];\n")
		split_name.write("\n")
		split_name.write("my $num_reads=" + str(options.split_gran) + ";\n")
		split_name.write("\n")
		split_name.write("my $num_splits = 0;\n")
		split_name.write("my $line_count=0;\n")
		split_name.write("\n")
		split_name.write("#Open input file\n")
		split_name.write("open(INPUT, $file);\n")
		split_name.write("\n")
		split_name.write("my $read_count = 0;\n")

		split_name.write("open (OUTPUT,\">input.$num_splits\");\n")
		split_name.write("$num_splits++;\n")
		split_name.write("while (my $line = <INPUT>) {\n")
		split_name.write("	chomp $line;\n")
		split_name.write("	#FASTQ files begin sequence with '@' character\n")
		split_name.write("	#If line begins with '@' then it is a new sequence and has 3 lines in between\n")
		split_name.write("	if ($line =~ /^[@]/ and $line_count % 4 ==0){\n")
		split_name.write("		#Check if the new sequence should be placed in a new file, otherwise place it in same file\n")
		split_name.write("		if ($read_count == $num_reads){\n")
		split_name.write("			close(OUTPUT);\n")
		split_name.write("			open(OUTPUT,\">input.$num_splits\");\n")
		split_name.write("			print OUTPUT $line;\n")
		split_name.write("			print OUTPUT \"\\n\";\n")
		split_name.write("			$num_splits++;\n")
		split_name.write("			$read_count = 0;\n")
		split_name.write("		}	\n")
		split_name.write("		else{\n")
		split_name.write("			print OUTPUT $line;\n")
		split_name.write("			print OUTPUT \"\\n\";\n")
		split_name.write("			$read_count++;\n")
		split_name.write("		}\n")
		split_name.write("	}\n")
		split_name.write("	#place all other lines in FASTQ file under same sequence\n")
		split_name.write("	else {\n")
		split_name.write("		print OUTPUT $line;\n")
		split_name.write("		print OUTPUT \"\\n\";\n")
		split_name.write("	}\n")
		split_name.write("\n")
		split_name.write("	$line_count++;\n")
		split_name.write("}\n")
		split_name.write("print $num_splits;\n")
		split_name.write("\n")
		split_name.write("close(INPUT);\n")
		split_name.write("	\n")
	finally:
		split_name.close()
		
		
		
def write_makeflow(destination):
	if destination is None:
		makeflow = sys.stdout
	else:
		makeflow = open(destination, 'w')
	
	try:
		inputlist = ""
		outputlist = ""
		num_splits = count_splits(options.split_gran, options.query)
		for i in range(num_splits):
	        	inputlist = inputlist + "input." + str(i) + " "
	        	outputlist = outputlist + "output." + str(i) + " "
		#Here we actually start generating the Makeflow
		#How to get inputs
		makeflow.write(inputlist+ ": " + options.query + " "+ split_name + "\n")
		makeflow.write("\tLOCAL perl " + split_name + " %s %s\n"%(options.query, options.split_gran))
		
		#How to get outputs
		for i in range(num_splits):
			
	        	makeflow.write("output." + str(i) + ": input." + str(i) + " " + "blasr ./lib/ \n")
	        	makeflow.write("\t LD_LIBRARY_PATH=\"./lib/\""
					+ " input." + str(i)
					+ " " + options.reference
					+" "+ ' '.join(args)
					+ " > output." + str(i)
                  	                + "\n")

		#How to concatenate outputs
		makeflow.write(options.output + ": " + outputlist + "\n")
		makeflow.write("\tLOCAL cat"+ " " + outputlist + " >"+ options.output+ "\n")

	
	finally:
			makeflow.close()

def main():
			
	
	write_makeflow(options.makeflow)
	write_split_name_reduce(split_name)
	
if __name__ == "__main__":
    main()
			
