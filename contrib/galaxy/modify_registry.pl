#!/usr/bin/perl
#
#Copyright (C) 2022 The University of Notre Dame
#This software is distributed under the GNU General Public License.
#See the file COPYING for details.
#

use strict;

if ($#ARGV != 0) {
	print "Usage: perl modify_registry.pl <registry>\n";
	exit 1;
}

my $in = shift;
my $out = "tmp_registry";

my $import = 0;
my $datatypes = 0;
my $mime = 0;
my $sniff = 0;

#Open input file
open(INPUT, $in);
open (OUTPUT,">$out");
while (my $line = <INPUT>) {
	chomp $line;
	if ($line =~ /^import/)
	{
		$import = 1;
	}
	elsif ($import == 1)
	{
		$import = 0;
		print OUTPUT "import cctools\n";
	}

	if ($line =~ /self.datatypes_by_extension = {$/)
	{
		$datatypes = 1
	}
	elsif ($datatypes == 1){
		if ($line =~ /}/){
			$datatypes = 0;
			print OUTPUT "\t\t\t\t'makeflowlog' : cctools.MakeflowLog(),\n";
			print OUTPUT "\t\t\t\t'wqlog'       : cctools.WorkQueueLog(),\n";
		}
	}

	if ($line =~ /self.mimetypes_by_extension = {$/)
	{
		$mime = 1
	}
	elsif ($mime == 1){
		if ($line =~ /}/){
			$mime = 0;
			print OUTPUT "\t\t\t\t'makeflowlog' : 'text/plain',\n";
			print OUTPUT "\t\t\t\t'wqlog'       : 'text/plain',\n";
		}
	}

	if ($line =~ /self.sniff_order = \[$/)
	{
		$sniff = 1
	}
	elsif ($sniff == 1){
		if ($line =~ /\]/){
			$sniff = 0;
			print OUTPUT "\t\t\t\tcctools.MakeflowLog(),\n";
			print OUTPUT "\t\t\t\tcctools.WorkQueueLog(),\n";
		}
		elsif (not $line =~ /\(\),/){
			$line = $line + ",";
		}
	}

	print OUTPUT "$line\n";
}
close(OUTPUT);
close(INPUT);
