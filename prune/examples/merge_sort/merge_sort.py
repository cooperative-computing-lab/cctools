#!/usr/bin/env python

# Copyright (c) 2010- The University of Notre Dame.
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

from prune import client
import os

#Prune data is stored in base_dir
prune = client.Connect(base_dir = os.environ["HOME"] + '/.prune')

###### Import sources stage ######
# D1 and D2 are handlers for our input files
D1 = prune.file_add( 'nouns.txt' )
D2 = prune.file_add( 'verbs.txt' )

# E1 is a handler for an environment specification. We will deal with
# environments in a further example, and for now we simply use prune's nil
E1 = prune.nil

###### Sort stage ######
# We define the first task, which sorts the file D1 (that is, nouns.txt)
# In the command line, D1 is mapped to the parameter 'input.txt'.
# The return value D3 represents the output file 'output.txt'
D3, = prune.task_add(returns=['output.txt'],
                     env=E1,
                     cmd='/bin/sort input.txt > output.txt',
                     args=[D1],
                     params=['input.txt'])

# Similarly, we define the second task, which sorts the file D2 (verbs.txt)
# Note that in the command line, D2 is also mapped to the parameter
# 'input.txt', and the output is also named 'output.txt'. This is ok, as all
# prune tasks are executed in their own sandbox.
D4, = prune.task_add(returns=['output.txt'],
                     env=E1,
                     cmd='sort input.txt > output.txt',
                     args=[D2],
                     params=['input.txt'])

###### Merge stage ######
# In the third task we combine the files D3 and D4 into the merged output D5.
# Note that D3 is mapped to input1.txt, D4 to input2.txt, and the output D5 to
# merged_output.txt
D5, = prune.task_add(returns=['merged_output.txt'],
                     env=E1,
                     cmd='sort -m input*.txt > merged_output.txt',
                     args=[D3,D4],
                     params=['input1.txt','input2.txt'])

###### Execute the workflow ######
# So far we have only defined the workflow, but nothing has been executed yet.
# Now we execute the workflow locally in our computer...
prune.execute( worker_type='local', cores=8 )

###### Export final data ######
# ...and export the final result into the file merged_words.txt...
prune.export( D5, 'merged_words.txt' )

###### Export publishable workflow ######
# ...and the workflow, complete with the original inputs and intermidiate files
# so that other can reproduce and modify our results:
prune.export( D5, 'merge_sort.prune', lineage=2 )

