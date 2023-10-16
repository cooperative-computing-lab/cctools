# Prune User's Manual

## Overview

Prune is a system for executing and precisely preserving scientific workflows.

Every task to be executed in a workflow is wrapped in a functional interface
and coupled with a strictly defined environment. The task is then executed by
Prune rather than the user to ensure reproducibility.

As a scientific workflow evolves in a Prune repository, a growing but immutable
tree of derived data is created. The provenance of every item in the system can
be precisely described, facilitating sharing and modification between
collaborating researchers, along with efficient management of limited storage
space.  Collaborators can verifiy research results and easily extend them at a
granularity that makes sense to users, since the granularity of each task was
chosen by a scientist rather than captured at the level of system calls.

## Getting Started

### Installing Prune

Prune is part of the [Cooperating Computing
Tools](http://ccl.cse.nd.edu/software). Follow the [installation instructions](../install/index.md) to setup CCTools required for
running Prune.


### Prune Example Workflow: Merge Sort

In this first example our workflow processes two text files, `nouns.txt` and
`verbs.txt` which contain a word per line, and produces the file
`merge_output.txt`, with all the words from the input files sorted
alphabetically.

- Inputs: [nouns.txt](examples/merge_sort/nouns.txt) [verbs.txt](examples/merge_sort/verbs.txt)
- Workflow: [merge_sort.py](examples/merge_sort/merge_sort.py)

```python
# merge_sort.py

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

```

After running the workflow with:

```sh
$ python merge_sort.py
```

the merged results can be found in a file called `merged_words.txt` and the
file `merge_sort.prune` contains a sharable package that describes the full
workflow.

If you try to execute the workflow again, **prune** finds the previous computed
results, and does not need to recompute them:

```sh
$ python merge_sort.py

Working from base directory: /home/btovar/.prune/
Allocating 8 local workers.
.Nothing to execute.

Export (merged_words.txt) contains the following objects...
file: 609507ca8c80d6f30c9df119766a5ac43690cc11 259

Export (merge_sort.prune) contains the following objects...
task: 42d1a7dc8c80af695882031cf8a5651c78d28ca6 sort -m input*.txt > merged_output.txt
task: 49e7ac81c83a6dac1c2a3d538b94c109b30c47d6 /bin/sort input.txt > output.txt
task: 319418e43783a78e3cb7e219f9a1211cba4b3b31 sort input.txt > output.txt
file: 29ae0a576ab660cb17bf9b14729c7b464fa98cca 144
file: 48044131b31906e6c917d857ddd1539278c455cf 115
Export description: pathname=merge_sort.prune prid(s)=48044131b31906e6c917d857ddd1539278c455cf {'lineage': 2}
Export results: duration=0.000000 size=1883 file_cnt=2 task_cnt=3 temp_cnt=0 more_cnt=0
```


## Prune Example Workflow: High Energy Physics (HEP)

The Merge Sort example above did not specify an environment. A different
workflow (involving High Energy Physics) uses [Umbrella](../umbrella/index.md) to
specify and create the appropriate environment for individual workflow tasks:

- Environment definition: [cms.umbrella](examples/hep/cms.umbrella)
- Input files: [digitize.sh](examples/hep/digitize.sh)[reconstruct.sh](examples/hep/reconstruct.sh)[simulate.sh](examples/hep/simulate.sh)
- Workflow: [hep.py](examples/hep/hep.py)


The script command to specify an Umbrella environment looks like this:

```python
E1 = prune.envi_add(engine='umbrella',
                    spec='cms.umbrella',
                    sandbox_mode='parrot',
                    log='umbrella.log',
                    cms_siteconf='SITECONF.tar.gz',
                    cvmfs_http_proxy='http://eddie.crc.nd.edu:3128',
                    http_proxy='http://eddie.crc.nd.edu:3128' )
```

Execute the workflow with this command:

```sh
$ python hep.py
```

## Prune Example Workflow: U.S. Census

The U.S. Census workflow demonstrates the scalability of Prune by using [Work
Queue](../work_queue/index.md) to execute the workflow in a distributed manner, rather
than only executing with the local machine. The included census data is a
small simulation of a real census, but could be applied to the real U.S.
Censuses if available.

This example workflow differs mainly in the way the execute command is used in
the script:

`prune.execute( worker_type='work_queue', name='prune_census_example' )`

Now, running the workflow script initiates a Work Queue manager that will wait
for workers to attach to it in order to execute the tasks.

`python match_people.py`

The following command line instruction is one way to assign 10 workers to the
Work Queue manager:

`condor_submit_workers -N prune_census_example 10`

See the [Work Queue Manual](../work_queue/index.md) for more information on ways to
assign workers to execute tasks in the workflow.

(The hep.wq.py script, in the hep example folder, runs the HEP workflow using
Work Queue after submitting workers to the Work Queue manager with name
'prune_hep_example' instead of 'prune_census_example'.)

## For More Information

For the latest information about CCTools, please visit our [web
site](http://ccl.cse.nd.edu/) and subscribe to our [mailing
list](http://ccl.cse.nd.edu/software/help.shtml).


Prune is Copyright (C) 2022 The University of Notre Dame.  
All rights reserved.  
This software is distributed under the GNU General Public License.  
See the file COPYING for details.



**Last edited: September 2017**

