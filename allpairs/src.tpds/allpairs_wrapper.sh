#!/bin/bash 

# Command Line Input:
# $1: FNAME
# $2: FUN.exe
# $3: Chirp A Directory on this node /chirp/localhost/DIR
# $4: Chirp B Directory on this node /chirp/localhost/DIR
# $5: Matrix Host
# $6: Matrix Path
# $7: Matrix Height
# $8: Matrix Width
# $9: Matrix Y Offset
# $10: Matrix X Offset
# $11: First B Index (p)
# $12: First A Index (q)
# $13: Last B Index (r)
# $14: Last A Index (s)


host=`hostname`
chirp="/afs/nd.edu/user37/ccl/software/devel/bin/chirp"
parrot="/afs/nd.edu/user37/ccl/software/devel/bin/parrot"

mkdir $1
if [ $? -ne 0 ]; then exit 96; fi;
cd $1
if [ $? -ne 0 ]; then exit 97; fi;
if [ -f ../$1.func.tar ]; then 
    tar xf ../$1.func.tar;
    if [ $? -ne 0 ]; then exit 98; fi;
    rm ../$1.func.tar
    if [ $? -ne 0 ]; then exit 99; fi;
    if [ ! -f $2 ]; then 
	pwd
	ls
	ls ..
	exit 100
    fi;
fi

echo "Command: ../allpairs_multicore  -i $7 -w $8 -Y $9 -X ${10} -p ${11} -q ${12} -r ${13} -s ${14} $3 $4 ./$2 $5 $6"
../allpairs_multicore  -i $7 -w $8 -Y $9 -X ${10} -p ${11} -q ${12} -r ${13} -s ${14} $3 $4 ./$2 $5 $6

retval=$?


# Uncomment these lines to send output and error text for each job back to the files set up by condor.

#HACK to fix a bug with Condor eviction that only appears with Karen's jobs.
#OIFS=$IFS
#IFS="
#";

#for i in `ps xU cmoretti -o "%p %t %a" | grep karen.exe | grep -v grep`; do 
#    j=`echo ${i} |  awk -F: '{sub(/^ */,""); print;}'`
#    j=${j%% *};
#    ps -p $j -o "%t" | grep -q "-"
#    rv=$?
#    if((($rv == 0) && ($j != $$))); then
#	echo "killing off $j on `hostname`: $i"
#	tcsh -c "kill -HUP $j"; 
#    fi
#done
#IFS=$OIFS
#/HACK!

# If we didn't get a 0 return, remove our waste and exit with a status to requeue
if [ $retval -ne 0 ]; then
    #echo "Returned $retval"
    echo "CLA returned $retval. Cleaning up and exiting"
    cd ..;
    echo "Contents of execution directory upon exit:"
    ls

    rm -rf $1
    rm -rf ./parrotdir
    #sleep 300
    exit $retval
fi

cd ..

# Do some cleanup
rm -rf $1
rm -rf ./parrotdir


exit $retval

# vim: set noexpandtab tabstop=4:
