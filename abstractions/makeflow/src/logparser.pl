#!/usr/bin/perl
#Programmer: Andrew Thrasher
#Date: 10/12/2009
#Purpose: Parse makeflow log files to generate useful logs

$log = $ARGV[0];

open(LOGFILE, "<", $log);
open(SUMMARY, ">", $log.".summary");

$line = <LOGFILE>;
@fields = split(/ /, $line);
$timestart = $fields[0];

$lastline = `tail -n 1 $log`;
@lastfields = split(/ /, $lastline);
$timeend = $lastfields[0];

$totaltime = $timeend - $timestart;

print SUMMARY "Elapsed Time: ", int($totaltime/(24*60*60)),"+", ($totaltime/(60*60))%24,":", ($totaltime/60)%60,":", $totaltime%60, "\n";
print SUMMARY "Jobs Submitted: ", $fields[9], "\n";
print SUMMARY "Jobs Completed: ", $lastfields[6], "\n";
$goodput;
$badput;

#$line = <LOGFILE>;
#@fields = split(/ /, $line);
$num_tasks = $fields[9];
@tasks[$num_tasks]; 
#print "First task: ", $fields[1], "\n";
if($fields[2] == 1)
{
	#entered running state, store timestamp
	#print "task number: ", $fields[1], " running \n";
	$tasks[$fields[1]] = $fields[0];
}
elsif($fields[2] == 2)
{
	#completed
	#print "task number: ", $fields[1], " completed, with run time: ", int($fields[0]) - int($tasks[$fields[1]]), "\n";
	$goodput += ($fields[0] - $tasks[$fields[1]]);
}
elsif($fields[2] == 3 || $fields[2] == 4)
{
	#failed or aborted
	#print "task number: ", $fields[1], " failed or aborted \n";
	$badput += ($fields[0] - $tasks[$fields[1]]);
}
else
{
	#went back to waiting
	#print $fields[1], "\n";	
}


#Need to determine the total CPU time
#use number of tasks as indices to each task

while($line = <LOGFILE>)
{
	@fields = split(/ /, $line);

	#fields: timestamp, work item #, new state, jobID (random id returned by worker), # of nodes waiting, # of nodes running, # of nodes completed, # of nodes failed, # of nodes aborted, # of jobs counter

	
	if($fields[2] == 1)
	{
	        #entered running state, store timestamp
		#print "task number: ", $fields[1], " running \n";
	        $tasks[$fields[1]] = $fields[0];
	}
	elsif($fields[2] == 2)
	{
	        #completed
		#print "task number: ", $fields[1], " completed, with run time: ", int($fields[0]) - int($tasks[$fields[1]]), "\n";
	        #print "Begin time: ", $tasks[$fields[1]], " End Time: ", $fields[0], "\n";
		$goodput += ($fields[0] - $tasks[$fields[1]]);
	}
	elsif($fields[2] == 3 || $fields[2] == 4)
	{
	        #failed or aborted
		#print "task number: ", $fields[1], " failed or aborted \n";
	        $badput += ($fields[0] - $tasks[$fields[1]]);
	}
	else
	{
	        #went back to waiting

	}


}

print SUMMARY "Goodput: ", int($goodput/(24*60*60)),"+", ($goodput/(60*60))%24,":", ($goodput/60)%60,":", $goodput%60, "\n";
print SUMMARY "Badput: ",  int($badput/(24*60*60)),"+", ($badput/(60*60))%24,":", ($badput/60)%60,":", $badput%60, "\n";
$cpu = $goodput + $badput;
print SUMMARY "Total CPU time: ",  int($cpu/(24*60*60)),"+", ($cpu/(60*60))%24,":", ($cpu/60)%60,":", $cpu%60, "\n";


open (GNUPLOT, "|gnuplot");
print GNUPLOT <<EOPLOT;
set terminal postscript solid eps 24 color
set size 2.5,1
set output "$log.eps"
set xdata time
set ylabel "Jobs Submitted / Complete"
set y2label "Jobs Running"
set y2tics 
set bmargin 4
set style line 1 
set key outside
set timefmt "%s"
plot "$log" using 1:(\$6+\$7+\$8+\$9) title "Submitted" with lines lw 5, "" using 1:6 title "Running" with lines lw 5, "" using 1:7 title "Complete" with lines lw 5
EOPLOT
close(GNUPLOT);

#plot "$log.makeflowlog" using 1:(\$5+\$6+\$7+\$8+\$9) title "Submitted" with lines lw 5, "" using 1:6 title "Running" with lines lw 5 axes x1y2, "" using 1:7 title "Complete" with lines lw 5
