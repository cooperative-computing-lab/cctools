use strict;
use warnings;

my $SCRIPT_FILE = "./amazon_ec2.sh";
my $BATCH_JOB_FILE = "batch_job_amazon.c";
my $OUTPUT_FILE = "batch_job_amazon.c.new";
my $BATCH_JOB_SCRIPT_STRING_VAR_NAME = "amazon_ec2_script";

open(my $SCRIPT_FP, '<', $SCRIPT_FILE) or die "Couldn't open $SCRIPT_FILE";
open(my $BATCH_JOB_FP, '<', $BATCH_JOB_FILE) or die "Couldn't open $BATCH_JOB_FILE";
open(my $OUTPUT_FP, '>', $OUTPUT_FILE) or die "Couldn't open $OUTPUT_FILE";

sub escape_line {
    chomp $_[0];
    $_[0] =~ s/\\/\\\\/g;
    $_[0] =~ s/'/\\'/g;
    $_[0] =~ s/"/\\"/g;
}

my $within_variable = 0;
while (my $line = <$BATCH_JOB_FP>) {
    if ($line =~ /$BATCH_JOB_SCRIPT_STRING_VAR_NAME ="/) {
        print $OUTPUT_FP $line;
        $within_variable = 1;
        next;
    }
    elsif ($within_variable and $line =~ /^";/) {
        while (my $script_line = <$SCRIPT_FP>) {
            escape_line($script_line);
            print $OUTPUT_FP $script_line . "\\n\\\n";
        }
        print $OUTPUT_FP "\";\n";
        $within_variable = 0;
        next;
    }

    print $OUTPUT_FP $line if $within_variable != 1;
}

close($OUTPUT_FP);
close($SCRIPT_FP);
close($BATCH_JOB_FP);
`mv $OUTPUT_FILE $BATCH_JOB_FILE`;
