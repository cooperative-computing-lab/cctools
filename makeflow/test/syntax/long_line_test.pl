#!/usr/bin/perl -w

# Print out a ridiculously long makeflow, test test our ability
# to parse very large lines.

print "all: ";

for($i=0;$i<1000000;$i++) {
	print "word$i ";
}

print "\n";
print "\tls\n";

