#!/usr/bin/env python

# Copyright (C) 2022- The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

# This example shows some of the data handling features of taskvine.
# It performs a BLAST search of the "Landmark" model organism database.
# It works by constructing tasks that download the blast executable
# and landmark database from NCBI, and then performs a short query.

# Each task in the workflow performs a query of the database using
# 16 (random) query strings generated at the manager.
# Both the downloads are automatically unpacked, cached, and shared
# with all the same tasks on the worker.

import taskvine as vine
import sys
import random

# Permitted letters in an amino acid sequence
amino_letters="ACGTUiRYKMSWBDHVN"

# Number of characters in each query
query_length = 128

# Number of queries in each task.
query_count = 16

# Number of tasks to generate
task_count = 1000

# Create a query string consisting of
# {query_count} sequences of {query_length} characters.

def make_query_text():
    return "".join(
        ">query\n"+"".join(
            random.choice(amino_letters)
            for x in range(query_length))+"\n"
        for y in range(query_count)
        )

if __name__ == "__main__":
    m = vine.Manager()
    print(f"TaskVine listening on {m.port}")

    print(f"Declaring files...")

    blast_url = m.declare_url("https://ftp.ncbi.nlm.nih.gov/blast/executables/blast+/LATEST/ncbi-blast-2.13.0+-x64-linux.tar.gz")
    blast = m.declare_untar(blast_url)

    landmark_url = m.declare_url("https://ftp.ncbi.nlm.nih.gov/blast/db/landmark.tar.gz")
    landmark = m.declare_untar(landmark_url)

    m.enable_peer_transfers()
    
    print(f"Declaring tasks...")

    for i in range(task_count):
        query = m.declare_buffer(make_query_text())

        t = vine.Task("blastdir/ncbi-blast-2.13.0+/bin/blastp -db landmark -query query.file")

        t.add_input(query, "query.file", cache=False )
        t.add_input(blast, "blastdir", cache=True )

        t.add_input(landmark, "landmark", cache=True )
        t.set_env_var("BLASTDB", value="landmark")
        t.set_cores(1)

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
