#!/usr/bin/perl

my $output = shift;
if (-e $output) {
	unlink($output) || die "could not delete $output";
} 
open $OUT,'>>',$output or die("Could not open " + $output + " file.");;

$file = 0;
$max = scalar @ARGV;

while ($in = shift) {
	++$file;

	open(IN,$in) or die("Could not open " + $in + " file.");
	@size = <IN>;
	$lines = @size;
	close(IN);

	open(IN,$in) or die("Could not open " + $in + " file.");
	$print = 1;
	while(my $line = <IN>) {
		if (($line =~ /BLAST/) and ($file ne 1)) { $print = 0;}
		if (($line =~ /Query/) and ($print eq 0)) { $print = 1; }
		last if (($line =~ /^\s+Database:/) and ($file lt $max));
		if ($print gt 0){
			print { $OUT } $line;
		}
	}
	close (IN);
}
close (OUT);

