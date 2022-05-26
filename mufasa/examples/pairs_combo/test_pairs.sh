#!/bin/sh
INBOX=$1
PAIRS_FILE=input.tar.gz

function move_to_inbox(){
	cp $1 /tmp/$2
	mv /tmp/$2 $INBOX
}

function test_pairs(){
	for index in $(seq $1); do
		move_to_inbox $PAIRS_FILE $PAIRS_FILE$index
	done
}

test_pairs $2
