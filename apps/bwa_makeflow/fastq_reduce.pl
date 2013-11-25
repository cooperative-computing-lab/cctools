#!/usr/bin/perl
#Programmer: Brian Kachmarck
#Date: 7/28/2009
#Purpose: Split a FASTQ file into smaller files determined by the number of sequences input

use strict; 


my $numargs = $#ARGV + 1;

my $file = $ARGV[0];

my $num_reads=50000;

my $num_outputs = 0;
my $line_count=0;

#Open input file
open(INPUT, $file);
#open(OUT_NUMSPLIT,"Numsplit.txt");

my $read_count = 0;
open (OUTPUT,">$file.$num_outputs");
$num_outputs++;
while (my $line = <INPUT>) {
	chomp $line;
	#FASTQ files begin sequence with '@' character
	#If line begins with '@' then it is a new sequence and has 3 lines in between
	if ($line =~ /^[@]/ and $line_count % 4 ==0){
		#Check if the new sequence should be placed in a new file, otherwise place it in same file
		if ($read_count == $num_reads){
			close(OUTPUT);
			open(OUTPUT, ">$file.$num_outputs");
			print OUTPUT $line;
			print OUTPUT "\n";
			$num_outputs++;
			$read_count = 0;
		}	
		else{
			print OUTPUT $line;
			print OUTPUT "\n";
			$read_count++;
		}
	}
	#place all other lines in FASTQ file under same sequence
	else {
		print OUTPUT $line;
		print OUTPUT "\n";
	}

	$line_count++;
}
if ($num_outputs != 0){
	print $num_outputs;
}
else{
	print $num_outputs;
}

#close(OUT_NUMSPLIT);
close(INPUT);
