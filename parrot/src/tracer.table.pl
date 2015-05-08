#!/usr/bin/perl

if($ARGV[0] eq "table") {
	$dotable = 1;
} elsif($ARGV[0] eq "header") {
	$doheader = 1;
} else {
	die "Use: $0 <table|header>\n";
}

$bits = $ARGV[1];

if($dotable) {
	print "static const char * syscall${bits}_names[] = {\n";
}

$n = 0;
while(<STDIN>) {
	next if /^\s*#/; # skip comments
	next if /^\s*$/; # skip empty

	($number,$abi,$name,$entry) = split;

	if($dotable) {
		while($n < $number) {
			print "\t\"unknown${n}\",\n";
			$n++;
		}
		print "\t\"${name}\",\n";
		$n++;
	} elsif ($doheader) {
		# Some system calls are repeated, define only the first instance (i.e. don't redefine)...
		print "#ifndef SYSCALL${bits}_${name}\n";
		print "\t#define SYSCALL${bits}_${name} ${number}\n";
		print "#endif\n";
		if ($n < $number) {
			$n = $number;
		}
	}
}

if($doheader) {
	print "#define SYSCALL${bits}_MAX ${n}\n";
}

if($dotable) {
	print "};\n";
}

# vim: set noexpandtab tabstop=4:
