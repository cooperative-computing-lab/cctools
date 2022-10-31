#!/usr/bin/env python

# Copyright (C) 2022- The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

# This example shows some of the data handling features of taskvine.
# It performs a BLAST search of the "Landmark" model organism database.
# It works by constructing tasks that download the blast executable
# and landmark database from NCBI, and then performs a short query.

# The query is provided by a string (but presented to the task as a file.)
# Both the downloads are automatically unpacked, cached, and shared
# with all the same tasks on the worker.

# Originally written in C (vine_example_blast.c)

from taskvine import *

import os
import sys
import errno

query_string = ">P01013 GENE X PROTEIN (OVALBUMIN-RELATED)\n\
		QIKDLLVSSSTDLDTTLVLVNAIYFKGMWKTAFNAEDTREMPFHVTKQESKPVQMMCMNNSFNVATLPAE\n\
		KMKILELPFASGDLSMLVLLPDEVSDLERIEKTINFEKLTEWTNPNTMEKRRVKVYLPQMKIEEKYNLTS\n\
		VLMALGMTDLFIPSANLTGISSAESLKISQAVHGAFMELSEDGIEMAGSTGVIEDIKHSPESEQFRADHP\n\
		FLFLIKHNPTNTIVYFGRYWSP\n"

BLAST_URL = "https://ftp.ncbi.nlm.nih.gov/blast/executables/blast+/LATEST/ncbi-blast-2.13.0+-x64-linux.tar.gz"

LANDMARK_URL = "https://ftp.ncbi.nlm.nih.gov/blast/db/landmark.tar.gz"

if __name__ == '__main__':
    try:
        m = Manager(port = VINE_DEFAULT_PORT)
    except IOError as e:
        print("couldn't create manager:",e.errno)
        sys.exit(1)
    print("listening on port",m.port)
    
    m.enable_debug_log("manager.log")
    m.set_scheduler(VINE_SCHEDULE_FILES)

    for i in range(10):
        t = Task("blastdir/ncbi-blast-2.13.0+/bin/blastp -db landmark -query query.file")
        
        t.add_input_buffer(query_string,"query.file", flags=VINE_NOCACHE)
        t.add_input_url(BLAST_URL,"blastdir", flags=VINE_CACHE|VINE_UNPACK )
        t.add_input_url(LANDMARK_URL,"landmark", flags=VINE_CACHE|VINE_UNPACK )
        t.set_env_var("BLASTDB",value="landmark")
        
        task_id = m.submit(t)
        
        print("submitted task (id#",task_id,"):",t.command)
    
    print("waiting for tasks to complete...")

    while not m.empty():
        t = m.wait(5)
        if t:
            r = t.result
            id = t.id

            if r==VINE_RESULT_SUCCESS:
                print("task",id,"output:",t.std_output)
            else:
                print("task",id,"failed:",r.result_string)
            del t

    print("all tasks complete!")

    del m

    sys.exit(0)