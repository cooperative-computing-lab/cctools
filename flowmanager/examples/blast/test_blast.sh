#!/bin/sh
INBOX=$1
BLAST_FILE=input.tar.gz

function move_to_inbox(){
	cp $1 /tmp/$2
	mv /tmp/$2 $INBOX
}

function test_blast(){
	for index in $(seq $1); do
		move_to_inbox $BLAST_FILE $BLAST_FILE$index
	done
}

test_blast $2
