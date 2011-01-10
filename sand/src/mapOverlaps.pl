#!/usr/bin/perl

use strict;
use warnings;

if (@ARGV != 1)
{
	print STDERR "Usage: mapOverlaps.pl <overlap file> \n";
	exit(1);
}
if (! -e $ARGV[0])
{
	print STDERR "Overlap file ".$ARGV[0]." does not exist!\n";
	exit(1);
}
open(FILE, $ARGV[0]) || die "Could not open overlap file ".$ARGV[0]."\n";
my $afr;
my $bfr;
while (my $line = <FILE>)
{
	if ($line =~ /([ab]fr)\:(.+)\n/)
	{
		my $f = $1;
		#$1 contains either afr or bfr
		#$2 contains the ID 
			my $x = $2;
			$x =~ s/.*,//g; 
			print "$f:$x\n";
	}
	else { 

		print $line;
	}

}
print STDERR "Done.\n";
close(FILE);
