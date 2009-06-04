#!/bin/sh

# Check arguments
if [ $# -ne 5 ]; then
    echo 1>&2 "usage: $0 phase scratch_dir <phase_options>"
    echo 1>&2 "  map    options: mid nmappers nreducers"
    echo 1>&2 "  reduce options: rid nmappers nreducers"
    echo 1>&2 "  merge  options: wid nmappers nreducers"
    exit 1
fi

phase=$1
scratch_dir=$2
wid=$3
nmappers=$4
nreducers=$5

# Add current directory to PATH
export PATH=$(pwd):$PATH

# Select parrot to use
parrot='parrot_hdfs'
if [ ! -x $parrot ]; then
    parrot='parrot'
fi

if [ ! -d $scratch_dir ]; then
    if ! $parrot_hdfs mkdir -p $scratch_dir; then
	exit 2
    fi
fi

# Execute MapReduce phase
case $phase in
	map)
	# Perform Map phase
	if ! $parrot mr_map $scratch_dir $wid $nmappers $nreducers; then
	    exit 3
	fi
	;;
	reduce)
	# Perform Reduce phase
	tarfile="reduce.input.$wid.tar"
	if [ -r $tarfile ]; then
	    if ! tar xf $tarfile; then
		exit 4
	    fi
	    if ! rm -f $tarfile; then
		exit 5
	    fi
	fi

	if ! $parrot mr_merge $scratch_dir $wid $nmappers $nreducers; then
	    exit 6
	fi

	rm -f reduce.input.$wid.[0-9]*

	if ! $parrot mr_reduce $scratch_dir $wid $nmappers $nreducers; then
	    exit 7
	fi
	;;
	merge)
	# Perform Merge phase
	if ! $parrot mr_merge $scratch_dir $wid $nmappers $nreducers; then
	    exit 8
	fi

	rm -f merge.output.[0-9]*
	;;
	*)
	echo 1>&2 "unknown phase: $phase"
	exit 9
	;;
esac

# Cleanup (only if we are in Condor)
if [ ! -z $_CONDOR_SCRATCH_DIR ]; then
    rm -f parrot parrot_hdfs mr_map mr_reduce mr_merge inputfile mapper reducer
fi
exit 0
