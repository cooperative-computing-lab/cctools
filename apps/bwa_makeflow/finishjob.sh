#!/bin/bash
source job.params

export PATH="/afs/nd.edu/user37/condor/software/bin:$PATH"
export PATH="/afs/nd.edu/user37/condor/software/sbin:$PATH"

if [[ $EMAIL != "TRUE" ]]; then
	EMAIL="FALSE"
fi

date +"%F %R:%S"                  >  $ID.complete

echo "Congratulations, job $ID finished.  To view a summary of this job, go to: https://biocompute.cse.nd.edu/viewjob.php?id=$ID" > mailtext.txt

if [[ $EMAIL == "TRUE" ]]; then 
	echo "tried to send mail to $ADDRESS" >> $ID.total
	echo "ran mail $ADDRESS -s \"Job $ID Finished!\" < mailtext.txt" >> $ID.total
	mail $ADDRESS -s "Job $ID Finished!" < mailtext.txt >> $ID.total 
	MAILOUT=$?
	if [ $MAILOUT != 0 ]; then
		echo "Mail failed with code $MAILOUT" >> $ID.total
	fi
fi

find . -name "$ID.input.0.*" -exec rm {} \;
find . -name "$ID.input.1.*" -exec rm {} \;
find . -name "$ID.input.2.*" -exec rm {} \;
find . -name "$ID.output.*" -exec rm {} \; 
find . -name "$ID.error.*" -exec rm {} \; 
find . -name "$ID.total.*" -exec rm {} \; 
