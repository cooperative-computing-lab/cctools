#!/usr/bin/perl

# This script converts a script on stdin to a C string on the output
# that can be embedded into a C program.

use strict;
use warnings;

print "\"\\\n";

sub escape_line {
    chomp $_[0];
    $_[0] =~ s/\\/\\\\/g;
    $_[0] =~ s/'/\\'/g;
    $_[0] =~ s/"/\\"/g;
}

while(<STDIN>) {
    escape_line($_);
    print "$_\\n\\\n";
}
print "\";\n";
