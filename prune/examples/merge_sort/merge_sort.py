#!/usr/bin/env cctools_python
# CCTOOLS_PYTHON_VERSION 2.7 2.6

# Copyright (c) 2010- The University of Notre Dame.
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

from prune import client
from os.path import expanduser


HOME = expanduser("~")
prune = client.Connect(base_dir = HOME+'/.prune') #Prune data is stored in base_dir

###### Import sources stage ######
E1 = prune.nil
D1 = prune.file_add( 'nouns.txt' )
D2 = prune.file_add( 'verbs.txt' )

###### Sort stage ######
D3, = prune.task_add( returns=['output.txt'],
	env=E1,	cmd='sort input.txt > output.txt',
	args=[D1], params=['input.txt'] )
D4, = prune.task_add( returns=['output.txt'],
	env=E1, cmd='sort input.txt > output.txt',
	args=[D2], params=['input.txt'] )

###### Merge stage ######
D5, = prune.task_add(
	returns=['merged_output.txt'], env=E1,
	cmd='sort -m input*.txt > merged_output.txt',
	args=[D3,D4], params=['input1.txt','input2.txt'] )

###### Execute the workflow ######
prune.execute( worker_type='local', cores=8 )

###### Export final data ######
prune.export( D5, 'merged_words.txt' )

###### Export publishable workflow ######
prune.export( D5, 'merge_sort.prune', lineage=2 )
