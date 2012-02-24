#!/usr/bin/perl
# Generating two 10,000-base random DNA sequences who are reverse complement to
# each other. The two sequences are saved in file "random.seq" and
# "random_revcom.seq" respectively.
use strict;
use warnings;
use Switch;

my $sequence_length = 100000;
my $filename = "random.seq";
my $filename_revcom = "random_revcom.seq";

my $random_number;
my $char;
my $i;


# Check sequence_length argument
my $numArgs;
$numArgs = $#ARGV+1;
if ($numArgs == 1) {
	$sequence_length = $ARGV[0];
}

# Generate a random sequence and write it to file
open MYFILE, ">$filename" or die $!;
for($i = 0; $i < $sequence_length; $i++) {
	$random_number = int(rand(4));
	switch($random_number) {
		case 0 	{$char = 'A'}
		case 1	{$char = 'T'}
		case 2 	{$char = 'G'}
		case 3 	{$char = 'C'}
		else 	{print "Something is wrong with the random number generator...";exit(1)}
	}
	print MYFILE $char;
}
close(MYFILE);
print "Random sequence of length $sequence_length is successfully generated in file $filename.\n";

# Read in the original sequence
open SEQFILE, "$filename" or die $!;
my $seq = <SEQFILE>;
close(SEQFILE);

if(length($seq) != $sequence_length) {
	print length($seq) . " bases are read from the original sequence.\n";
	print "Sequence length is not as expected ($sequence_length).\n";
	exit(1);
}

# Get the reverse complement of the original sequence
my $seq_revcom = reverse $seq;
$seq_revcom =~ tr/ACGT/TGCA/;

# Write the reverse complement to file
open MYFILE, ">$filename_revcom" or die $!;
print MYFILE $seq_revcom;
close(MYFILE);
print "The reverse complement of the random sequence is successfully generated in file $filename_revcom.\n";

exit(0);
