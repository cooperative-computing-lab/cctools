#!/usr/bin/perl
#
#Copyright (C) 2016- The University of Notre Dame
#This software is distributed under the GNU General Public License.
#This program analyzes the works of William Shakespeare as provided
#by MIT and parses each play using the MIT format.
#

use strict; 
use Scalar::Util qw(looks_like_number);
use Math::Complex;

my $num_args = $#ARGV + 1;
if($num_args != 1) {
	print "Must specify: input file.\n";
	exit 1;
}

my $in = $ARGV[0];
my %char_hash = ();
my $top_count_chars = 0;
my @top_chars = ();
my $curr_char;
my $title;
my $lines = 0;
open(INPUT, $in);
open(OUTPUT, ">>", "characters_$in");

while (my $line = <INPUT>) {
	chomp $line;
	my @words = split(" ",$line);

	$line = uc($line);
	if($lines == 0) {
		$title = $line;
		print(STDERR "Now Reading: $title\n");
		print(OUTPUT "$title\n");
	}
	if($words[0] eq "SCENE" or $words[0] eq "ACT" or $words[0] eq "PROLOGUE" or $words[0] eq "EPILOGUE" or $words[0] eq "SONG" or $words[0] eq "SONG.") {
		$lines++;
	}
	elsif(@words == 1 and $words[0] =~ m/^[^a-z]*$/ and length($words[0]) >= 3) {
		$curr_char = $words[0];
		if(exists $char_hash{$curr_char}) {
			$char_hash{$curr_char}++;
		}
		else {
			$char_hash{$curr_char} = 1;
		}
	}
	$lines++;
}

close(INPUT);
foreach my $val (sort keys(%char_hash)) {
	my $count = $char_hash{$val};
	print(OUTPUT "$val:$count\n");
	if($count == $top_count_chars) {
		push(@top_chars, $val);
	}
	elsif($count > $top_count_chars) {
		@top_chars = ($val);
		$top_count_chars = $count;
	}
}
close(OUTPUT);
