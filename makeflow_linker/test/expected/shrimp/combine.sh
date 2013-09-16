#!/bin/bash

if [ ! $# == 4 ]; then
	echo "Usage: $0 job_id output_option genome_length"
 	exit
fi

id=$1
format=$2
genome_len=$3
querytype=$4
prettyprint="prettyprint-"$querytype

if [ $format == "raw" ]; then
	echo raw
	cat output.* > $id.output
    exit 0
elif [ $format == "probcalc" ]; then
	echo probcalc
	cat output.* > rawoutput
	./probcalc $genome_len rawoutput > $id.output	
elif [ $format == "prettyprint" ]; then
	echo prettyprint
	cat output.* > rawoutput
	./probcalc $genome_len rawoutput > probcalcoutput	
	./$prettyprint probcalcoutput $id.input.1 $id.input.${querytype}fasta > $id.output
else
	echo Error: output option does not exist!
fi

exit 0

