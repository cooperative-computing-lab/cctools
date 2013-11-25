#!/usr/bin/perl

my $output = shift;
if (-e $output) {
	unlink($output) || die "Could not delete $output";
} 

open $OUT,'>>',$output or die("Could not open " + $output + " file.");;

$file = 0;

while ($in = shift) {
	++$file;

	open(IN,$in) or die("Could not open " + $in + " file.");
	$print = 0;
	while(my $line = <IN>) {
		if (($line =~ /^@/) and ($file ne 1)) { $print = 0;}
		elsif (($line =~ /main/)) { $print = 0; }
		elsif (($line =~ /M::/)) { $print = 0; }
		else { $print = 1; }
		if ($print gt 0){
			print { $OUT } $line;
		} 
	}
	close (IN);
}
close (OUT);
