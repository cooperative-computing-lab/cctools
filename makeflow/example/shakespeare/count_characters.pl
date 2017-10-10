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

my @plays = ("alls_well_that_ends_well.txt", "henry_iv_part_1.txt", "julius_caesar.txt", "othello.txt", "the_merchant_of_venice.txt", "twelfth_night.txt", "a_midsummer_nights_dream.txt", "henry_iv_part_2.txt", "king_john.txt", "pericles_prince_of_tyre.txt", "the_merry_wives_of_windsor.txt", "two_gentlemen_of_verona.txt", "antony_and_cleopatra.txt", "henry_viii.txt", "king_lear.txt", "richard_iii.txt", "the_taming_of_the_shrew.txt", "winters_tale.txt", "as_you_like_it.txt", "henry_vi_part_1.txt", "loves_labours_lost.txt", "richard_ii.txt", "the_tempest.txt", "coriolanus.txt", "henry_vi_part_2.txt", "macbeth.txt", "romeo_and_juliet.txt", "timon_of_athens.txt", "cymbeline.txt", "henry_vi_part_3.txt", "measure_for_measure.txt", "text_analyzer.pl", "titus_andronicus.txt", "hamlet.txt", "henry_v.txt", "much_ado_about_nothing.txt", "the_comedy_of_errors.txt", "troilus_and_cressida.txt");

my %hash = ();
my $top_count_chars = 0;
my @top_chars = ();

foreach my $play (sort @plays) {
	open(INPUT, "characters_$play");
	my $lines = 0;
	my $title;
	while (my $line = <INPUT>) {
		chomp $line;
		my @words = split(":",$line);
		$line = uc($line);
		if($lines == 0) {
			$title = $words[0];
		}
		else {
			my $curr_char = "$title - $words[0]";
			if(exists $hash{$curr_char}) {
				$hash{$curr_char} += $words[1];
			}
			else {
				$hash{$curr_char} = $words[1];
			}
		}
		$lines++;
	}
	close(INPUT);
}

foreach my $val (sort keys(%hash)) {
	my $count = $hash{$val};
	if($count == $top_count_chars) {
		push(@top_chars, $val);
	}
	elsif($count > $top_count_chars) {
		@top_chars = ($val);
		$top_count_chars = $count;
	}
}

open(OUTPUT, ">>", "top_character.txt");
print(OUTPUT "\nCHARACTER(S) WITH MOST LINES:\n");
print(STDERR "\nCHARACTER(S) WITH MOST LINES:\n");
foreach my $val (@top_chars) {
	print(OUTPUT "\t$val : $top_count_chars\n\n");
	print(STDERR "\t$val : $top_count_chars\n\n");
}
close(OUTPUT);
