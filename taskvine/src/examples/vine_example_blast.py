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

import taskvine as vine
import sys

query_string = """>P01013 GENE X PROTEIN (OVALBUMIN-RELATED)
QIKDLLVSSSTDLDTTLVLVNAIYFKGMWKTAFNAEDTREMPFHVTKQESKPVQMMCMNNSFNVATLPAE
KMKILELPFASGDLSMLVLLPDEVSDLERIEKTINFEKLTEWTNPNTMEKRRVKVYLPQMKIEEKYNLTS
VLMALGMTDLFIPSANLTGISSAESLKISQAVHGAFMELSEDGIEMAGSTGVIEDIKHSPESEQFRADHP
FLFLIKHNPTNTIVYFGRYWSP"""

blast_url = "https://ftp.ncbi.nlm.nih.gov/blast/executables/blast+/LATEST/ncbi-blast-2.13.0+-x64-linux.tar.gz"

landmark_url = "https://ftp.ncbi.nlm.nih.gov/blast/db/landmark.tar.gz"

if __name__ == "__main__":
    try:
        m = vine.Manager()
    except IOError as e:
        print("couldn't create manager:", e.errno)
        sys.exit(1)
    print("listening on port", m.port)

    m.enable_debug_log("manager.log")
    m.set_scheduler(vine.VINE_SCHEDULE_FILES)

    for i in range(10):
        t = vine.Task(
            "blastdir/ncbi-blast-2.13.0+/bin/blastp -db landmark -query query.file"
        )

        t.add_input_buffer(query_string, "query.file", cache=True)
        t.add_input(vine.FileUntgz(vine.FileURL(blast_url)), "blastdir", cache=True )
        t.add_input(vine.FileUntgz(vine.FileURL(landmark_url)), "landmark", cache=True )
        t.set_env_var("BLASTDB", value="landmark")

        task_id = m.submit(t)

        print("submitted task (id# " + str(task_id) + "):", t.command)

    print("waiting for tasks to complete...")

    while not m.empty():
        t = m.wait(5)
        if t:
            r = t.result
            id = t.id

            if r == vine.VINE_RESULT_SUCCESS:
                print("task", id, "output:", t.std_output)
            else:
                print("task", id, "failed:", t.result_string)

    print("all tasks complete!")
