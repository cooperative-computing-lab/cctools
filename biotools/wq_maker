#!/usr/bin/env perl

use strict "vars";
use strict "refs";
use warnings;
use Time::HiRes qw(gettimeofday); 
use FindBin;
use lib "$FindBin::Bin/../lib";
use lib "$FindBin::Bin/../perl/lib";

BEGIN{
   if (not ($ENV{CGL_SO_SOURCE})) {
      $ENV{CGL_SO_SOURCE} = "$FindBin::Bin/../lib/CGL/so.obo";
   }
   if (not ($ENV{CGL_GO_SOURCE})) {
      $ENV{CGL_GO_SOURCE} = "$FindBin::Bin/../lib/CGL/gene_ontology.obo"
   }
   
   #Handle CTRL+C
   $SIG{'INT'} = sub {
      print STDERR "\n\nMaker aborted by user!\n\n";
      exit (1);
   };    
   
   #supress warnings from storable module
   $SIG{'__WARN__'} = sub {
      warn $_[0] if ( $_[0] !~ /Not a CODE reference/ &&
		      $_[0] !~ /Can\'t store item / &&
		      $_[0] !~ /Find\:\:skip_pattern|File\/Find\.pm/
		    );
   };
}

use Cwd;
use Cwd qw(abs_path); 
use Getopt::Long qw(:config no_ignore_case);
use Iterator::Any;
use Error qw(:try);
use Error::Simple;
use Storable qw(nstore retrieve);
use work_queue;
use POSIX qw(ceil floor);
#external libraries below.
use Proc::Signal;
use GI;
use Fasta;
use ds_utility;
use GFFDB;
use Process::MpiChunk;
use Process::MpiTiers;

$| = 1; #turn on autoflush.

my $usage = "
Usage:
	 maker_wq [options] <maker_opts> <maker_bopts> <maker_exe>
     
Maker Options:

     -genome|g <filename> Specify the genome file.

     -predictor|p <type>  Selects the predictor(s) to use when building
                          annotations.  Defines a pool of gene models for
                          annotation selection.
                          types: snap
                                 augustus
                                 fgenesh
                                 genemark
                                 est2genome (Uses EST's directly)
                                 protein2genome (For Prokaryotic annotation only)
                                 model_gff (Pass through GFF3 annotations)
                                 pred_gff (Uses passed through GFF3 predictions)

                          Use a ',' to seperate types (nospaces)
                          i.e. -predictor=snap,augustus,fgenesh

     -RM_off|R           Turns all repeat masking off.

     -datastore/         Forcably turn on/off MAKER's use of a two deep datastore
      nodatastore        directory structure for output.  By default this option
                         turns on whenever there are more the 1,000 contigs in
                         the input genome fasta file.

     -base    <string>   Set the base name MAKER uses to save output files.
                         MAKER uses the input genome file name by default.

     -retry|r <integer>  Rerun failed contigs up to the specified count.

     -cpus|c  <integer>  Tells how many cpus to use for BLAST analysis.

     -force|f            Forces maker to delete old files before running again.
                         This will require all blast analyses to be re-run.

     -again|a            Caculate all annotations and output files again even if
                         no settings have changed. Does not delete old analyses.

     -evaluate|e         Run Evaluator on final annotations (under development).

     -CTL                Generate empty control files in the current directory.

     -help|?             Prints this usage statement.

Work Queue Options

     -port <integer>     Sets the port for work_queue to listen on (default: 9135)
 
     -fa   <integer>     Sets the work_queue fast abort option with the given multiplier. 

     -N <project>	 Sets the project name to <project>.

     -k <integer>	 Sets the number of sequences per task for remote computation. 
 
     -d <level>  	 Sets the debug flag for Work Queue and the CCTOOLS package. For all debugging output, try 'all'.

";

#------------------------------------ MAIN -------------------------------------

#---Process options on the command line 
my %OPT;
try{
    GetOptions("RM_off|R" => \$OPT{R},
	       "force|f" => \$OPT{force},
	       "genome|g=s" => \$OPT{genome},
	       "cpus|c=i" => \$OPT{cpus},
	       "predictor=s" =>\$OPT{predictor},
	       "retry=i" =>\$OPT{retry},
	       "evaluate" =>\$OPT{evaluate},
	       "again|a" =>\$OPT{again},
	       "check" =>\$OPT{check},
	       "base=s" =>\$OPT{out_name},
	       "datastore!" =>\$OPT{datastore},
	       "CTL" => sub {GI::generate_control_files(); exit(0);},
	       "help|?" => sub {print $usage; exit(0)},
	       "port=i" => \$OPT{port},
	       "fa=i" => \$OPT{fast_abort},
	       "N=s" => \$OPT{project},
	       "d=s" => \$OPT{debug},
 	       "k=i" => \$OPT{size}
	       );

} catch Error::Simple with{
    my $E = shift;
    print STDERR $E->{-text};
    die "\n\nMaker failed parsing command line options!\n" if();
};

if(!defined $OPT{size}){
	$OPT{size} = 1; 
}

#global variables 
my %CTL_OPT;
my $DS_CTL;
my $GFF_DB;
my $build;
my $iterator;
my $tiers_per_task = $OPT{size}; 
my $wkdir = get_wkdir();

try{
    #get arguments off the command line
    my @ctlfiles = @ARGV;
    if (not @ctlfiles) {
		if (-e "maker_opts.ctl" && -e "maker_bopts.ctl" && -e "maker_exe.ctl") {
			@ctlfiles = ("maker_opts.ctl", "maker_bopts.ctl", "maker_exe.ctl");
		} else {
			print STDERR "Maker control files not found\n";
			print STDERR $usage;
			exit(0);
		}
    }
    
    #--Control file processing
    #set up control options from control files
    %CTL_OPT = GI::load_control_files(\@ctlfiles, \%OPT, 1);
    
    #--open datastructure controller
    #This is where the output directory must be set
    $DS_CTL = ds_utility->new(\%CTL_OPT);
    
    #--set up gff database
    $GFF_DB = new GFFDB(\%CTL_OPT);
    $build = $GFF_DB->next_build;
    
    #---load genome multifasta/GFF3 file
    $iterator = new Iterator::Any( -fasta => $CTL_OPT{'genome'}, -gff => $CTL_OPT{'genome_gff'},);
} catch Error::Simple with{
    my $E = shift;
    print STDERR $E->{-text};
    my $code = 2;
    $code = $E->{-value} if (defined($E->{-value}));
    exit($code);
};

my $datastore = $DS_CTL->{root}; 
if(!-e $datastore) {
	`mkdir $datastore`;
}

my $total_tiers = GenTiers();
print time." :: Total number of tiers generated was $total_tiers \n";

###Begin WorkQueue section
my $wq = setup_workqueue();
create_worker_script();

my $last_annotated_tier = 0;
my $init_tiers_submit = get_initial_tiers_to_submit($total_tiers, $tiers_per_task); 
$last_annotated_tier = submit_tiers_to_annotate($wq, $total_tiers, $last_annotated_tier, $init_tiers_submit);
print time." :: Initial submission to Work Queue is $init_tiers_submit tiers\n";
my $tiers_submitted = $init_tiers_submit;

my %task_failures; 

my $annotated_tiers = 0;
while ($annotated_tiers < $tiers_submitted) {
	$annotated_tiers += $tiers_per_task * process_completed_tier_task($wq, 5);
	my $tiers_to_submit = get_tiers_to_submit($tiers_per_task);
	while ($tiers_to_submit > 0 && $tiers_submitted < $total_tiers) {
		print time." :: There is capacity to annotate $tiers_to_submit tiers.\n";
		my $remaining_tiers = $total_tiers - $tiers_submitted;	
		if ($tiers_to_submit > $remaining_tiers) {	
			$last_annotated_tier = submit_tiers_to_annotate($wq, $total_tiers, ($last_annotated_tier+1), $remaining_tiers);	
			$tiers_submitted += $remaining_tiers; 
		} else {
			$last_annotated_tier = submit_tiers_to_annotate($wq, $total_tiers, ($last_annotated_tier+1), $tiers_to_submit);	
			$tiers_submitted += $tiers_to_submit; 
		}
	}
}

delete_workqueue($wq);
print "\n".time." :: Tiers annotated :: $annotated_tiers in total \n";

#------------------------------- FUNCTION DEFINITIONS -------------------------------------
sub get_wkdir {
	my $wkdir = cwd();
	$wkdir .= "/";
	return $wkdir;
}

sub GenTiers {
	# $iterator is implicitly global
	my $tier_ds;
	my $numTiers = 0;
	while (my $fasta = $iterator->nextFasta() ) {
		$tier_ds = Process::MpiTiers->new({fasta =>$fasta,
			CTL_OPT => \%CTL_OPT,
			DS_CTL  => $DS_CTL,
			GFF_DB  => $GFF_DB,
			build   => $build},
		   '0',
		   'Process::MpiChunk'
		   );
		nstore \$tier_ds, ($numTiers."_todo.tier");
		$numTiers++;
	}
	return $numTiers;
}

sub setup_workqueue {
	if(defined($OPT{"debug"})){
		work_queue::cctools_debug_flags_set($OPT{"debug"}); 
		print time." :: Work Queue debug flags set.\n";
	}

	my $port = "9155";
	if(defined($OPT{"port"})) {
		$port = $OPT{"port"}; 
	} 
	my $wq = work_queue::work_queue_create($port);
	if(defined($wq)) {
		print time." :: Work Queue listening on port $port.\n";
	} else {
		print STDERR "Failed to create Work Queue on port $port.\n"; 
		exit(0);
	}
	if(defined($OPT{"fast_abort"})) {
		my $multiplier = $OPT{"fast_abort"}; 
		my $fa = work_queue::work_queue_activate_fast_abort($wq, $multiplier); 
		print time." :: Work Queue fast abort set to $multiplier.\n";
	}
	if(defined($OPT{"project"})) {
		work_queue::work_queue_specify_name($wq, $OPT{"project"});
		work_queue::work_queue_specify_master_mode($wq, 1);
		print time." :: Work Queue project name set to $OPT{\"project\"}.\n";
	}
	work_queue::work_queue_specify_log($wq, "maker_wq.stats");
	return $wq;
}

sub create_worker_script {
 my $worker_script = <<'END';
#!/usr/bin/env perl
use strict "vars";
use strict "refs";
use warnings;
use FindBin;
use vars qw($RANK $LOG $CMD_ARGS);

BEGIN {
   if (not ($ENV{CGL_SO_SOURCE})) {
	  $ENV{CGL_SO_SOURCE} = "$FindBin::Bin/../lib/CGL/so.obo";
   }
   if (not ($ENV{CGL_GO_SOURCE})) {
	  $ENV{CGL_GO_SOURCE} = "$FindBin::Bin/../lib/CGL/gene_ontology.obo"
   }
   
   $CMD_ARGS = join(' ', @ARGV);
   
   #Handle CTRL+C
   $SIG{'INT'} = sub {
	  print STDERR "\n\nMaker aborted by user!!\n\n" unless($main::qq);
	  exit (1);
   };    
   
   #supress warnings from storable module
   $SIG{'__WARN__'} = sub {
	  warn $_[0] if ( $_[0] !~ /Not a CODE reference/ &&
			  $_[0] !~ /Can\'t store item / &&
			  $_[0] !~ /Find\:\:skip_pattern|File\/Find\.pm/
			);
   };
  
  #output to log file of seq that caused rank to die
   $SIG{'__DIE__'} =
   sub {
      if (defined ($LOG) && defined $_[0]) {
	 my $die_count = $LOG->get_die_count();
	 $die_count++;
	 
	 $LOG->add_entry("DIED","RANK",$RANK);
	 $LOG->add_entry("DIED","COUNT",$die_count);
      }

      die $_[0]."\n".
	  "FATAL ERROR\n";
   };
}

my $link_output = `find -name "*" -exec ln -s {} . \\\;`;

my $numArgs = $#ARGV + 1;
if ($numArgs < 1) {
		print "Usage: maker_wq_worker <tier count>\n";
		exit;
}

my $counter = $ARGV[0];
my $tier;

use lib "./lib"; 
use Storable qw(retrieve);
use ds_utility;
use runlog;
use Error qw(:try);
use Error::Simple;
use Process::MpiTiers;
use Proc::Signal;

my @ts = @ARGV; 
foreach my $c (@ts) {
	my $inFile = $c."_todo.tier";
	my $total = scalar(@ts); 
	print "On component $c of $#ts, $total, $counter\n"; 

	#tier files were generated with the nstore command from the Storable Perl module
	#This simply stores a hash to disk in a convenient format. 
	$tier = ${retrieve($inFile)};
	$tier->CTL_OPT->{out_name} = "."; 
	$tier->CTL_OPT->{out_base} = "."; 
	$tier->CTL_OPT->{the_void} = ".";
	$tier->{the_void} = ".";  

	print time." :: Worker $counter :: starting \n";
	$tier->run_all;
	print time." :: Worker $counter :: finished \n";

	if($tier->failed) { print "EXIT:-1 \n"; last; }
	elsif($tier->terminated) { print "EXIT:1 \n"; next; }
}
END

 open(my $fh, '>maker_wq_worker');
 print $fh $worker_script;
 chmod(0755, $fh); 
 close($fh); 
 return;
}

sub get_tiers_to_submit {
	my ($tiers_in_task) = @_;
	my $tasks_to_submit = work_queue::work_queue_hungry($wq);
	my $tiers_to_submit = $tasks_to_submit * $tiers_in_task;

	return $tiers_to_submit;
}

sub get_initial_tiers_to_submit {
	my($total_tiers, $tiers_in_task) = @_;	
	my $total_tier_tasks = ceil($total_tiers / $tiers_in_task);
	my $initial_submit_tiers = ($total_tier_tasks <= 100 ? $total_tiers : 100*$tiers_in_task);

	return $initial_submit_tiers;
}

sub submit_tiers_to_annotate {
	my($wq, $total_tiers, $start_tier, $tiers_to_submit) = @_;
	if (($total_tiers - $start_tier) < 1) {
		return 0;
	} elsif (($total_tiers - $start_tier) < $tiers_to_submit) {
		$tiers_to_submit = $total_tiers - $start_tier;	
	} else {
		#do nothing..
	}
	my $end_tier = $start_tier + $tiers_to_submit - 1;
	my $tiers_in_task = $tiers_per_task;	
	while($start_tier <= $end_tier) {
		my $tiers_left = ($end_tier - $start_tier) + 1; 
		if ( $tiers_left < $tiers_per_task) {
			$tiers_in_task = $tiers_left; 
		}
		SubmitWQTask($wq, $start_tier, $tiers_in_task);
		$start_tier += $tiers_in_task;	
	}
	return $end_tier; #return last annotated tier.
}

sub SubmitWQTask {
	my($wq, $tier_to_submit, $tiers_in_task) = @_;
	if($tiers_in_task < 1) {
		print STDERR "Invalid query size. Should be at least 1.\n"; 
		exit(0);
	}
	
	my $task_command .= "find -name \"*.tar\" -exec tar --skip-old-files -x -f {} \\\;
	                     ./maker_wq_worker";
	my $query_seq = ""; 
	for(my $i = 0; $i < $tiers_in_task; $i++) {
		my $val = $tier_to_submit + $i; 
		$query_seq .= "$val "; 
	}
	if($query_seq eq ""){
		return 0; 
	}
	print time." :: Creating WQ task for query sequence(s) $query_seq\n";  
	my $task = work_queue::work_queue_task_create("$task_command $query_seq"); 
	work_queue::work_queue_task_specify_tag($task, "$tier_to_submit"); #parm2 must be a string
	

	for(my $i = 0; $i < $tiers_in_task; $i++){
		submit_input_base_files($task, $tier_to_submit);
		submit_input_tier_files($task, $tier_to_submit);
		configure_remote_tier_annotation($tier_to_submit);	
		specify_output_tier_files($task, $tier_to_submit);
		$tier_to_submit++; 
	}
	work_queue::work_queue_submit($wq, $task);
	print time." :: Submitted task with command: $task_command for query seq: $query_seq\n";

	return 1;
}

sub submit_input_base_files {
	my ($task, $tier) = @_;
	
	work_queue::work_queue_task_specify_file($task, "maker_wq_worker", "maker_wq_worker", 0, 1);
	
	#specify the MAKER library files 
	my $lib = "$FindBin::Bin";
	$lib =~ s/bin/lib/; 
	if(! -e "$wkdir/lib.tar") {
		`tar -C $lib/.. -cf $wkdir/lib.tar lib/`;	
		`tar -C $wkdir/perl_libs_install -rf $wkdir/lib.tar lib/`;	
	}
	work_queue::work_queue_task_specify_file($task, "lib.tar", "lib.tar", 0, 1);

	my $tierFile = $tier."_todo.tier";
	my $tier_ds = ${retrieve($tierFile)}; #$tier_ds holds all the information for task 
	submit_edb_input_files($task, \$tier_ds);
	submit_pdb_input_files($task, \$tier_ds);	
	submit_ddb_input_files($task, \$tier_ds);	
	submit_rdb_input_files($task, \$tier_ds);	
	submit_adb_input_files($task, \$tier_ds);	
	submit_input_files($task, \$tier_ds);	
	submit_executable_files($task, \$tier_ds);	
	nstore \$tier_ds, ($tierFile);
}

sub submit_input_tier_files {
	my ($task, $tier) = @_;
	my $tierFile = $tier."_todo.tier";
	work_queue::work_queue_task_specify_file($task, $wkdir.$tierFile, $tierFile, 0, 0);
}

sub submit_edb_input_files {
	my($task, $tier_ds) = @_;
	$tier_ds = $$tier_ds; #dereference $tier_ds since it is passed by reference
	if (defined $tier_ds->CTL_OPT->{_e_db}){
		my @ests = @{$tier_ds->CTL_OPT->{_e_db}};
		for(my $j = 0; $j < scalar(@ests); $j++){
			my $file = $ests[$j]; 
			my @fields = split(/\//, $file);
			if ($#fields > 1){
				$file = abs_path($file); 
			}
			@fields = split(/\//, $file);
			my $newfile = $fields[scalar(@fields) - 1];
			work_queue::work_queue_task_specify_file($task, $file, "$newfile", 0, 1);
			if ($#fields > 1) {
				$ests[$j] = $newfile;  
			}
		}
		$tier_ds->CTL_OPT->{_e_db} = \@ests;
	}
}

sub submit_pdb_input_files {
	my($task, $tier_ds) = @_;
	$tier_ds = $$tier_ds; #dereference $tier_ds since it is passed by reference
	if (defined $tier_ds->CTL_OPT->{_p_db}){ 
		my @proteins = @{$tier_ds->CTL_OPT->{_p_db}}; 	
		for(my $j = 0; $j < scalar(@proteins); $j++){
			my $file = $proteins[$j];
			my @fields = split(/\//, $file);
			if ($#fields > 1){
				$file = abs_path($file); 
			}
			@fields = split(/\//, $file); 
			my $newfile = $fields[scalar(@fields) - 1]; 
			work_queue::work_queue_task_specify_file($task, $file, "$newfile", 0, 1); 
			if ($#fields > 1){
				$proteins[$j] = $newfile; 
			}
		}
		$tier_ds->CTL_OPT->{_p_db} = \@proteins; 
	}	
}

sub submit_ddb_input_files {
	my($task, $tier_ds) = @_;
	$tier_ds = $$tier_ds; #dereference $tier_ds since it is passed by reference
	if (defined $tier_ds->CTL_OPT->{_d_db}){
		my @d = @{$tier_ds->CTL_OPT->{_d_db}}; 	
		for(my $j = 0; $j < scalar(@d); $j++){
			my $file = $d[$j]; 
			my @fields = split(/\//, $file);
			if ($#fields > 1){
				$file = abs_path($file); 
			}
			@fields = split(/\//, $file); 
			my $newfile = $fields[scalar(@fields) - 1];
			work_queue::work_queue_task_specify_file($task, $file, "$newfile", 0, 1); 
			if ($#fields > 1){
				$d[$j] = $newfile; 
			}
		}
		$tier_ds->CTL_OPT->{_d_db} = \@d; 
	}		
}

sub submit_rdb_input_files {
	my($task, $tier_ds) = @_;
	$tier_ds = $$tier_ds; #dereference $tier_ds since it is passed by reference
	if (defined $tier_ds->CTL_OPT->{_r_db}){
		my @r = @{$tier_ds->CTL_OPT->{_r_db}}; 	
		for(my $j = 0; $j < scalar(@r); $j++){
			my $file = $r[$j]; 
			my @fields = split(/\//, $file);
			if ($#fields > 1){
				$file = abs_path($file); 
			}
			@fields = split(/\//, $file); 
			my $newfile = $fields[scalar(@fields) - 1]; 
			work_queue::work_queue_task_specify_file($task, $file, "$newfile", 0, 1); 
			if ($#fields > 1){
				$r[$j] = $newfile; 
			}
		}
		$tier_ds->CTL_OPT->{_r_db} = \@r; 
	}
}

sub submit_adb_input_files {
	my($task, $tier_ds) = @_;
	$tier_ds = $$tier_ds; #dereference $tier_ds since it is passed by reference 
	if (defined $tier_ds->CTL_OPT->{_a_db}){
		my @a = @{$tier_ds->CTL_OPT->{_a_db}}; 	
		for(my $j = 0; $j < scalar(@a); $j++){
			my $file = $a[$j];
			my @fields = split(/\//, $file);
			if ($#fields > 1){
				$file = abs_path($file); 
			}
			@fields = split(/\//, $file); 
			my $newfile = $fields[scalar(@fields) - 1]; 
			work_queue::work_queue_task_specify_file($task, $file, "$newfile", 0, 1); 
			
			if ($#fields > 1){
				$a[$j] = $newfile; 
			}
		}
		$tier_ds->CTL_OPT->{_a_db} = \@a; 
	}
}

sub submit_input_files {
	my($task, $tier_ds) = @_;
	$tier_ds = $$tier_ds; #dereference $tier_ds since it is passed by reference 
	my @inputs = ("_est", "_altest", "_repeat_protein", "_protein", "_est_reads", "protein", "repeat_protein", "genome", "est", "est_reads", "altest" ); 
	foreach my $input (@inputs){
		if (! defined $tier_ds->CTL_OPT->{$input}){next;}
		my $file = $tier_ds->CTL_OPT->{$input}; 
		my @fields = split(/\//, $file); 
		my $newfile = $fields[scalar(@fields) - 1]; 
		if(-e $file){
			$file = abs_path($file); 
			work_queue::work_queue_task_specify_file($task, $file, "$newfile", 0, 1); 
		}
		$tier_ds->CTL_OPT->{$input} = $newfile; 
	}
}

sub submit_executable_files {	
	my($task, $tier_ds) = @_;
	$tier_ds = $$tier_ds; #dereference $tier_ds since it is passed by reference 
	my @files = ('snaphmm',  'SEEN_file', 'rmlib');
	foreach my $x (@files){ 
		my $file = $tier_ds->CTL_OPT->{$x}; 
		my @fields = split(/\//, $file); 
		my $newfile = $fields[scalar(@fields) - 1]; 
		if(-e $file){
			$file = abs_path($file); 
			work_queue::work_queue_task_specify_file($task, $file, "$newfile", 0, 1);
			$tier_ds->CTL_OPT->{$x} = "./$newfile"; 
		}

	}	 
	
	my @execs = ('formatdb', 'blastall', '_formater', '_tblastx', 'tblastx', 'blastx', 'xdformat', 'exonerate', 'snap', 'RepeatMasker', 'blastn', 'formatdb', 'gmhmme3', '_blastx', '_blastn', 'probuild', 'augustus', 'gmhmme3', 'gmhmmp', 'fgenesh', 'twinscan');
	foreach my $v (@execs){
		my $file = $tier_ds->CTL_OPT->{$v}; 
		if (! -e $file){
			next;
		}
		$file = abs_path($file); 
		my @fields = split(/\//, $file); 
		my $prefix = ""; 
		for (my $j = 0; $j < scalar(@fields) - 1; $j++) {
			$prefix .= "/".$fields[$j];
		}
		my $newfile = $fields[scalar(@fields) - 1]; 
		my $folder = $fields[scalar(@fields) - 2]; 
		if(-e $file){
			if (! -e "$v.tar"){
				`tar -C $prefix/.. -cf $wkdir/$v.tar $folder`; 
			}
			work_queue::work_queue_task_specify_file($task, "$v.tar", "$v.tar", 0, 1);
			$tier_ds->CTL_OPT->{$v} = "$folder/$newfile"; 
		}
	}
}

sub configure_remote_tier_annotation{
	my($tier) = @_;
	my $tierFile = $tier."_todo.tier";
	my $tier_ds = ${retrieve($tierFile)};
	$tier_ds->CTL_OPT->{out_name} = "."; 
	$tier_ds->CTL_OPT->{out_base} = "."; 
	$tier_ds->CTL_OPT->{the_void} = ".";
	my $file = $tier_ds->DS_CTL->{log}; 
	my @fields = split(/\//, $file);
	$file = $fields[scalar(@fields) - 1]; 
	$tier_ds->DS_CTL->{log} = "./$file"; 
	my $root = $tier_ds->DS_CTL->{root}; 
	$tier_ds->DS_CTL->{root} = "./"; 
	$tier_ds->DS_CTL->{ds_object}->{_root} = "./"; 
	$tier_ds->{the_void} = "."; 
	$tier_ds->CTL_OPT->{CWD} = ".";  
	$tier_ds->GFF_DB->{dbfile} = "./";
	$tier_ds->CTL_OPT->{_TMP} = "./";  
	
	nstore \$tier_ds, ($tierFile); #store the modified variables to the tier file that is transmitted to workers	
}

sub specify_output_tier_files {
	my ($task, $tier) = @_;
	
	my $tierFile = $tier."_todo.tier";
	my $tier_ds = ${retrieve($tierFile)}; #$tier_ds holds all the information for task 
	if(!exists($tier_ds->{VARS}->{fasta})){
		print STDERR "Fasta doesn't exist\n"; 
		exit(0);
	}
	#need to know the remote directory name, this is generated from the sequence header
	my $fasta = Fasta::ucFasta(\$tier_ds->{VARS}->{fasta}); #build uppercase fasta
	my $q_def = Fasta::getDef(\$fasta); #Get fasta header
	my $seq_id = Fasta::def2SeqID($q_def); #Get sequence identifier
	my $root = $tier_ds->DS_CTL->{root}; 
	my $newfile = $root."/$seq_id";
	my $ds_flag  = (exists($tier_ds->CTL_OPT->{datastore})) ? $tier_ds->CTL_OPT->{datastore} : 1;
	if ($ds_flag) {
		use Digest::MD5 qw(md5_hex);

		my $dir = ""; 
		my($digest) = uc md5_hex($seq_id); #the hex string, uppercased
		for(my $j = 0; $j < 2; $j++) {
			$dir .= substr($digest, $j*2, 2) ."/";
		}
		$dir .= $seq_id . "/";
		work_queue::work_queue_task_specify_file($task, "$datastore/$dir", $dir, 1, 0); 
	} else {
		work_queue::work_queue_task_specify_file($task, "$datastore/$seq_id", $seq_id, 1, 0); 
	}
}

sub delete_workqueue {
	my($wq) = @_;
	work_queue::work_queue_delete($wq);
}

sub process_completed_tier_task {
	my($wq, $timeout) = @_;

	#some thing is launching "maintain.pl" which becomes a zombie script, it needs to die before we run WQ wait. 
	Proc::Signal::reap_children_by_name(9, 'maintain.pl');
	my $t = work_queue::work_queue_wait($wq, $timeout); 
	if(defined($t)) {
		my $counter = $t->{tag}; 
		my $output = $t->{output}; 
		my $retStatus = index($output, "EXIT:");
		if ($retStatus != -1) {
			$retStatus = substr($output, $retStatus+5, 2);
			$retStatus = sprintf("%d", $retStatus);
		}
		if($retStatus == 0 || $retStatus == 1) {
			print time." :: Finished WQ task with starting tier $counter.\n";
			unlink "$counter\_todo.tier" or warn "Could not unlink $counter\_todo.tier: $!\n";
			work_queue::work_queue_task_delete($t);
			return 1;
		} else { #tier resubmission on failure 
			$task_failures{$counter} += 1; 
			if($task_failures{$counter} < 5){
				print time." :: Failed, resubmitting WQ task with starting tier $counter\n";
				work_queue::work_queue_submit($wq, $t);
			} else{
				print time." :: WQ task with starting tier $counter has failed too many times\n"; 
				work_queue::work_queue_task_delete($t);
			}
		}
	} 
	return 0;
}

exit(0);
