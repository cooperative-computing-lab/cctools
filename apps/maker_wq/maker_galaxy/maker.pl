#
#Copyright (C) 2012- The University of Notre Dame
#This software is distributed under the GNU General Public License.
#See the file COPYING for details.
#

#!/usr/bin/perl

use Getopt::Long;

my %opt;
GetOptions("genome=s" => \$opt{genome},
	   "ests=s" => \$opt{ests},
	   "est_reads=s" => \$opt{est_reads},
	   "proteins=s" => \$opt{proteins},
	   "alt_est=s" => \$opt{alt_est},
	   "est_gff=s" => \$opt{est_gff},
	   "altest_gff=s" => \$opt{altest_gff},
	   "protein_gff=s" => \$opt{altest_gff},
	   "repeat_protein=s" => \$opt{repeat_protein},
	   "rmlib=s" => \$opt{rmlib},
	   "rm_gff=s" => \$opt{rm_gff},
	   "model_gff=s" => \$opt{model_gff},
	   "pred_gff=s" => \$opt{pred_gff},
	   "other_gff=s" => \$opt{other_gff},
	   "organism_type=s" => \$opt{organism_type},
	   "snap" => \$opt{snap},
	   "snap_hmm=s" => \$opt{snap_hmm},
	   "augustus" => \$opt{augustus},
	   "augustus_hmm=s" => \$opt{augustus_hmm},
	   "genemark" => \$opt{genemark},
	   "genemark_hmm=s" => \$opt{genemark_hmm},
	   "fgenesh" => \$opt{fgenesh},
	   "fgenesh_param=s" => \$opt{fgenesh_param},
	   "est2genome" => \$opt{est2genome},
	   "protein2genome" => \$opt{protein2genome},
	   "output=s" => \$opt{output},	
	   "error=s" => \$opt{error}
	);

foreach $el (keys %opt){
	if($opt{$el} eq "None"){
		$opt{$el} = "";
	}
	$val = $opt{$el}; 
}

#generate the generic control files
$result = `maker_wq -CTL`; 
#read the whole control file in
open(FILE, "maker_opts.ctl"); 
my $text = do{local $/;<FILE>};
close FILE;
#now we need to place all of the options into the control file
$val = $opt{genome}; 
$text =~ s/genome:/genome:$val/; 
$val = $opt{ests};
$text =~ s/est:/est:$val/;
$val = $opt{est_reads};
$text =~ s/est_reads:/est_reads:$val/;
$val = $opt{altest};
$text =~ s/altest:/altest:$val/;
$val = $opt{est_gff}; 
$text =~ s/est_gff:/est_gff:$val/;
$val = $opt{altest_gff}; 
$text =~ s/altest_gff:/altest_gff:$val/;
$val = $opt{protein}; 
$text =~ s/protein:/protein:$val/;
$val = $opt{protein_gff}; 
$text =~ s/protein_gff:/protein_gff:$val/;
$val = $opt{repeat_protein}; 
if($val ne ""){
	$text =~ s/repeat_protein:.*\/te_proteins.fasta/repeat_protein:$val/; 
}
$val = $opt{rmlib};
$text =~ s/rmlib:/rmlib:$val/;
$val = $opt{rm_gff}; 
$text =~ s/rm_gff:/rm_gff:$val/;
$val = $opt{organism_type};
$text =~ s/organism_type:eukaryotic/organism_type:$val/;
$val = $opt{snaphmm};
$text =~ s/snaphmm:/snaphmm:$val/;
$val = $opt{augustus_hmm}; 
$text =~ s/augustus_species:/augustus_species:$val/; 
$val = $opt{genemark_hmm}; 
$text =~ s/gmhmm:/gmhmm:$val/;
$val = $opt{fgenesh_param}; 
$text =~ s/fgenesh_par_file:/fgenesh_par_file:$val/;
$val = $opt{model_gff};
$text =~ s/model_gff:/model_gff:$val/;
$val = $opt{pred_gff};
$text =~ s/pred_gff:/pred_gff:$val/; 
$pred  = ""; 
if($opt{snap}){
$pred .= "snap,";
}
if($opt{augustus}){
$pred .= "augustus,"; 
}
if($opt{genemark}){
$pred .= "genemark,"; 
}
if($opt{fgenesh}){
$pred .= "fgenesh,"; 
}
if($opt{est2genome}){
$pred .= "est2genome,"; 
}
if($opt{protein2genome}){
$pred .= "protein2genome,"; 
}
$text =~ s/predictor:/predictor:$pred/; 

`rm maker_opts.ctl`; 

open(FILE, ">", "maker_opts.ctl"); 
print FILE $text; 
close FILE;
#need a pre-designed maker_exe.ctl file that gives the locations of all the executables necessary 
`cp /path/to/maker_exe.ctl .`;
$error = $opt{error};
$output = $opt{output}; 
print STDERR "maker_wq -N biocompute-a -port -1 >& $error\n"; 
`maker_wq -N biocompute-a -port -1 >& $error`;
`touch output`;
print "find . -name \"*.gff\" -exec cat {} \\; >> $output"; 
`find . -name "*.gff" -exec cat {} \\; >> $output`;
