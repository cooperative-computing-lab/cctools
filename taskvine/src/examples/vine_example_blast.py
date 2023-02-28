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

query = """>P01013 GENE X PROTEIN (OVALBUMIN-RELATED)
QIKDLLVSSSTDLDTTLVLVNAIYFKGMWKTAFNAEDTREMPFHVTKQESKPVQMMCMNNSFNVATLPAE
KMKILELPFASGDLSMLVLLPDEVSDLERIEKTINFEKLTEWTNPNTMEKRRVKVYLPQMKIEEKYNLTS
VLMALGMTDLFIPSANLTGISSAESLKISQAVHGAFMELSEDGIEMAGSTGVIEDIKHSPESEQFRADHP
FLFLIKHNPTNTIVYFGRYWSP"""


if __name__ == "__main__":
    m = vine.Manager()
    print(f"listening on {m.port}")

    query_buffer = m.declare_buffer(query)

    blast_url = m.declare_url("https://ftp.ncbi.nlm.nih.gov/blast/executables/blast+/LATEST/ncbi-blast-2.13.0+-x64-linux.tar.gz")
    blast = m.declare_untar(blast_url)

    landmark_url = m.declare_url("https://ftp.ncbi.nlm.nih.gov/blast/db/landmark.tar.gz")
    landmark = m.declare_untar(landmark_url)

    for i in range(10):
        t = vine.Task("blastdir/ncbi-blast-2.13.0+/bin/blastp -db landmark -query query.file")

        t.add_input(query_buffer, "query.file", cache=True)
        t.add_input(blast, "blastdir", cache=True )

        t.add_input(landmark, "landmark", cache=True )
        t.set_env_var("BLASTDB", value="landmark")

        task_id = m.submit(t)
        print(f"submitted task {t.id}: {t.command}")

    print("waiting for tasks to complete...")
    while not m.empty():
        t = m.wait(5)
        if t:
            if t.successful():
                print(f"task {t.id} result: {t.std_output}")
            elif t.completed():
                print(f"task {t.id} completed with an executin error, exit code {t.exit_code}")
            else:
                print(f"task {t.id} failed with status {t.result_string}")

    print("all tasks complete!")
