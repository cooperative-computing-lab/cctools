#!/usr/bin/python

#
# This program scans a set of object (.o) files and produces
# a call graph showing the relationships between each modules.
# The command line arguments are just the files to scan,
# and the output is the graph, in the DOT graphviz language.
#
# Example use:
# ./make_call_graph.py makeflow/src/*.o | dot -T pdf > makeflow.pdf
#

import subprocess
import sys
import os
import collections

# fileof[symbol] -> filename
fileof = {}
# uses[filename][symbol] -> True if filename uses but does not define symbol
uses = collections.defaultdict(lambda: collections.defaultdict(lambda: False))
# links[source][target] -> True if module source calls module target
links = collections.defaultdict(lambda: collections.defaultdict(lambda: False))

print "digraph \"G\" {"
print "node [shape=box]"

# Pass 1: Run nm on each of the input files.
# Scrape out the T records, which indicate a symbol definition.
# Scrape out the U records, which indicate a symbol reference.

for file in sys.argv[1:]:
    filename = os.path.basename(file)
    p = subprocess.Popen(["/usr/bin/nm","-f","posix",file],stdout=subprocess.PIPE)
    for line in iter(p.stdout.readline,''):
        words = line.split(" ")
	symbol = words[0]
	symtype = words[1]

        if symtype=='T':
            fileof[symbol] = filename
        elif symtype=='U':
            uses[filename][symbol] = True

# Pass 2: Match up each undefined reference with its definition,
# and mark it in the links[source][target] dictionary.  (Could be
# more than one instance of a link.)

for file in uses.keys():
    for symbol in uses[file].keys():
        if symbol in fileof:
            source = file
            target = fileof[symbol]
	    if not links[source][target]:
                links[source][target] = True

# Pass 3: Print out each of the module-module links.

for source in links.keys():
    for target in links[source].keys():
        print "\"%s\" -> \"%s\"" % (source,target)

print "}"

