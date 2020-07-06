#!/usr/bin/perl

$num_args = $#ARGV + 1;
	
if ($num_args != 2 and $num_args != 3) {
    print "Usage: fastq_generator number_of_contigs length_of_contigs\n";
    print " -or-  fastq_generator number_of_contigs length_of_contigs ref.fastq\n";
    exit;
}

my $n = $ARGV[0];
my $m = $ARGV[1];
my $ref;
if ($num_args == 3){
	$ref = $ARGV[2];
}

@chars = ('A', 'G', 'C', 'T');
#Small quality scores set
@qual = ('(',')','*','+',',','-','.','/','0','1','2','3','4','5','6','7','8','9',':',';','<','=');
#Full quality scores set
#@qual = ('!','"','#','$','%','&','\'','(',')','*','+',',','-','.','/','0','1','2','3','4','5','6','7','8','9',':',';','<','=','>','?','@','A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T','U','V','W','X','Y','Z','[','\\',']','^','_','`','a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z','{','|','}','~');

my $contig;
my $RF;
if(defined $ref){
	open($RF, '<', $ref);
}

foreach my $i (1..$n) {
	print "\@SEQ_ID:$i:FLOW_LANE:1:$i:1:1 1:N:0\n";

	my $selection = 1;

	if(defined $RF) {
		if (!<$RF>) { #Skip name
			close($RF);
			open($RF, '<', $ref); # Restart Seed values
			<$RF> #Skip name again
		}
		$selection = rand($m);

		$contig = <$RF>;
		<$RF>; #Skip +
		<$RF>; #Skip score
		print substr($contig, 0, $selection-1);
	} 

	foreach my $j ($selection..$m) {
			print $chars[rand @chars];
	}
	print "\n";
	print "+\n";
	foreach my $j (1..$m) {
			print $qual[rand @qual];
	}
	print "\n";
}

close($RF);
