#!/bin/bash
#PRUNE_INPUTS File*
#PRUNE_OUTPUT merged_output.txt

sort -m $@ > merged_output.txt
