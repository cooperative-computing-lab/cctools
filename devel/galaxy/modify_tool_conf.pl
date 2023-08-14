#!/usr/bin/perl
#
#Copyright (C) 2022 The University of Notre Dame
#This software is distributed under the GNU General Public License.
#See the file COPYING for details.
#

use strict;

if ($#ARGV != 0) {
	print "Usage: perl modify_registry.pl <tool_conf>\n";
	exit 1;
}

my $in = shift;
my $out = "tmp_tool_conf";

#Open input file
open(INPUT, $in);
open (OUTPUT,">$out");
while (my $line = <INPUT>) {
	chomp $line;
	if ($line =~ /^<\/toolbox>/)
	{
		print OUTPUT "\t<section id=\"ndBioapps\" name=\"NDBioapps\">\n";
		print OUTPUT "\t\t<tool file=\"ndBioapps/makeflow_bwa_wrapper.xml\"/>\n";
		print OUTPUT "\t\t<tool file=\"ndBioapps/makeflow_gatk_wrapper.xml\"/>\n";
		print OUTPUT "\t<\/section>\n";
	}
	print OUTPUT "$line\n";
}
close(OUTPUT);
close(INPUT);
