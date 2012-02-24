#!/usr/bin/perl
# Verify the correctness of the generated "random.cand" according to the
# original sequence file "random.seq" and "random_revcom.seq".
use strict;
use warnings;

my $seq_file = "random.seq";
my $seq_revcom_file = "random_revcom.seq";
my $cand_file = "random.cand";

my $read_size = 100;
my $kmer_size = 22;
my $count = 0;
my $seq;
my $seq_revcom;
my $line;
my $kmer1;
my $kmer2;
my $tmp_read;

# Read in the two sequences
open SEQFILE, "$seq_file" or die $!;
$seq = <SEQFILE>;
print length($seq) . " bases are read from $seq_file.\n";
close(SEQFILE);

open SEQFILE, "$seq_revcom_file" or die $!;
$seq_revcom = <SEQFILE>;
print length($seq) . " bases are read from $seq_revcom_file.\n";
close(SEQFILE);

# Verify the correctness of .cand file
open CANDFILE, "$cand_file" or die $!;
while($line = <CANDFILE>) {
	chomp($line);
	# The format of each line in the .cand file is: 
	# read1_name   read2_name   direction   start_position_in_read1   start_position_in_read2
	if($line =~ /(\S+)\s+(\S+)\s+(1|-1)\s+(\d+)\s+(\d+)/) {
		$count++;
	
		# "read_name" denotes the position of this read in its original
		# sequence. Since we have two original sequence here, in order to
		# locate the reads from the correct original sequence, we added the
		# prefix "rc" for reads from one of the sequence.
		if(substr($1, 0, 2) ne "rc") {
			$kmer1 = substr $seq, $1+$4, $kmer_size;
		} else {
			$kmer1 = substr $seq_revcom, substr($1, 2)+$4, $kmer_size;
		}

		if($3 eq "1") {
			# When direction equals 1
			if(substr($2, 0, 2) ne "rc") {
				$kmer2 = substr $seq, $2+$5, $kmer_size;
			} else {
				$kmer2 = substr $seq_revcom, substr($2, 2)+$5, $kmer_size;
			}

			if($kmer1 ne $kmer2) {
				print "Incorrect match found at line $count\n";
				goto failure;
			}
		} elsif ($3 eq "-1") {
			# When direction equals -1
			if(substr($2, 0, 2) ne "rc") {
				$tmp_read = substr $seq, $2, 100;
				$kmer2 = substr $tmp_read, -$5-$kmer_size, $kmer_size;
			} else {
				$tmp_read = substr $seq_revcom, substr($2,2), 100;
				$kmer2 = substr $tmp_read, -$5-$kmer_size, $kmer_size;
			}

			my $revcom = reverse $kmer2;
			$revcom =~ tr/ACGT/TGCA/;
			if($kmer1 ne $revcom) {
				print "Incorrect match found at line $count\n";
				goto failure;
			}
		} else {
			print "Invalid direction value at line $count\n";
			goto failure;
		}
	} elsif ($line eq "EOF") {
		goto success;
	} else {
		$count++;
		print "Invalid candidate format at line $count\n";
		goto failure;
	}
}	

success:
	close(CANDFILE);
	print "$count candidate pairs have been verified as correct candidates.\n";
	exit(0);

failure:
	close(CANDFILE);
	exit(1);

