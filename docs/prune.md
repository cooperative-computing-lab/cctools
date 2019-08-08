# Prune User's Manual

**Last edited: September 2017**

Prune is Copyright (C) 2014- The University of Notre Dame.  
All rights reserved.  
This software is distributed under the GNU General Public License.  
See the file COPYING for details.

## Overview⇗

Prune is a system for executing and precisely preserving scientific workflows.
Every task to be executed in a workflow is wrapped in a functional interface
and coupled with a strictly defined environment. The task is then executed by
Prune rather than the user to ensure reproducibility. As a scientific workflow
evolves in a Prune repository, a growing but immutable tree of derived data is
created. The provenance of every item in the system can be precisely
described, facilitating sharing and modification between collaborating
researchers, along with efficient management of limited storage space.
Collaborators can verifiy research results and easily extend them at a
granularity that makes sense to users, since the granularity of each task was
chosen by a scientist rather than captured at the level of system calls.

## Installing Prune⇗

Prune is part of the [Cooperating Computing
Tools](http://ccl.cse.nd.edu/software). The CCTools package can be downloaded
from [this web page](http://ccl.cse.nd.edu/software/download). Follow the
[installation instructions](../install.html) to setup CCTools required for
running Prune.

You will also need to set PYTHONPATH so that Prune modules from cctools can be
imported in a Python script:

`export PYTHONPATH=${PYTHONPATH}:${HOME}/cctools/lib/python2.7/site-packages`

## Prune Example Workflow: Merge Sort⇗

Change your working directory to the Merge Sort example folder with a command
like this: `cd ~/cctools.src/prune/examples/merge_sort/`

You will find two text files there (nouns.txt and verbs.txt) along with a
Python script containing the Prune workflow called merge_sort.py which looks
like this:

`###### Connect to a Prune repository ###### ###### (Default location:
~/.prune) ###### from prune import client from os.path import expanduser HOME
= expanduser("~") prune = client.Connect(base_dir = HOME+'/.prune') #Prune
data is stored in base_dir ###### Import sources stage ###### E1 = prune.nil
D1 = prune.file_add( 'nouns.txt' ) D2 = prune.file_add( 'verbs.txt' ) ######
Sort stage ###### D3, = prune.task_add( returns=['output.txt'], env=E1,
cmd='sort input.txt > output.txt', args=[D1], params=['input.txt'] ) D4, =
prune.task_add( returns=['output.txt'], env=E1, cmd='sort input.txt >
output.txt', args=[D2], params=['input.txt'] ) ###### Merge stage ###### D5, =
prune.task_add( returns=['merged_output.txt'], env=E1, cmd='sort -m input*.txt
> merged_output.txt', args=[D3,D4], params=['input1.txt','input2.txt'] )
###### Execute the workflow ###### prune.execute( worker_type='local', cores=8
) ###### Export final data ###### prune.export( D5, 'merged_words.txt' )
###### Export publishable workflow ###### prune.export( D5,
'merge_sort.prune', lineage=2 ) `

With CCTools libraries in your PYTHONPATH, you can execute the workflow with
this command:

`python merge_sort.py`

The merged results can be found in a file called merged_words.txt and the file
merge_sort.prune contains a sharable package that describes the full workflow.

## Prune Example Workflow: High Energy Physics (HEP)⇗

The Merge Sort example above did not specify an environment. A different
workflow (involving High Energy Physics) uses [Umbrella](umbrella.html) to
specify and create the appropriate environment for individual workflow tasks.
It can be found in the following working directory.

`cd ~/cctools.src/prune/examples/hep/`

The script command to specify an Umbrella environment looks like this:

`E1 = prune.envi_add( engine='umbrella', spec='cms.umbrella',
sandbox_mode='parrot', log='umbrella.log', cms_siteconf='SITECONF.tar.gz',
cvmfs_http_proxy='http://eddie.crc.nd.edu:3128',
http_proxy='http://eddie.crc.nd.edu:3128' )`

Execute the workflow with this command:

`python hep.py`

## Prune Example Workflow: U.S. Census⇗

The U.S. Census workflow demonstrates the scalability of Prune by using [Work
Queue](../workqueue/) to execute the workflow in a distributed manner, rather
than only executing with the local machine. The included census data is a
small simulation of a real census, but could be applied to the real U.S.
Censuses if available.

`cd ~/cctools.src/prune/examples/census/`

This example workflow differs mainly in the way the execute command is used in
the script:

`prune.execute( worker_type='work_queue', name='prune_census_example' )`

Now, running the workflow script initiates a Work Queue master that will wait
for workers to attach to it in order to execute the tasks.

`python match_people.py`

The following command line instruction is one way to assign 10 workers to the
Work Queue master:

`condor_submit_workers -N prune_census_example 10`

See the [Work Queue Manual](workqueue.html) for more information on ways to
assign workers to execute tasks in the workflow.

(The hep.wq.py script, in the hep example folder, runs the HEP workflow using
Work Queue after submitting workers to the Work Queue master with name
'prune_hep_example' instead of 'prune_census_example'.)

## For More Information⇗

For the latest information about CCTools, please visit our [web
site](http://ccl.cse.nd.edu/) and subscribe to our [mailing
list](http://ccl.cse.nd.edu/software/help.shtml).

