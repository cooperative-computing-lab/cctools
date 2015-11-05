use strict;
use warnings;

my $SCRIPT_FILE = "amazon_ec2.sh";
my $OUTPUT_FILE = "converted_file";

open(my $SCRIPT_FP, '<', $SCRIPT_FILE);
open(my $OUTPUT_FP, '>', $OUTPUT_FILE);
while (my $line = <$SCRIPT_FP>) {
    chomp $line;
    $line =~ s/\\/\\\\/g;
    $line =~ s/'/\\'/g;
    $line =~ s/"/\\"/g;
    print $line . "\\n\\\n";
    print $OUTPUT_FP $line . "\\n\\\n";
}
