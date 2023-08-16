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

import ndcctools.taskvine as vine
import random
import argparse
import getpass

# Permitted letters in an amino acid sequence
amino_letters = "ACGTUiRYKMSWBDHVN"

# Number of characters in each query
query_length = 128


def make_query_text(query_count):
    """Create a query string consisting of {query_count} sequences of {query_length} characters."""
    queries = []
    for i in range(query_count):
        query = "".join(random.choices(amino_letters, k=query_length))
        queries.append(query)
    return ">query\n" + "\n".join(queries) + "\n"


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="vine_example_blast.py",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description="""This example shows some of the data handling features of taskvine.
It performs a BLAST search of the "Landmark" model organism database.
It works by constructing tasks that download the blast executable
and landmark database from NCBI, and then performs a short query.

Each task in the workflow performs a query of the database using
16 (random) query strings generated at the manager.
Both the downloads are automatically unpacked, cached, and shared
with all the same tasks on the worker.""",
    )

    parser.add_argument(
        "--task-count",
        nargs="?",
        type=int,
        help="the number of tasks to generate.",
        default=10,
    )
    parser.add_argument(
        "--query-count",
        nargs="?",
        type=int,
        help="the number of queries to generate per task.",
        default=16,
    )
    parser.add_argument(
        "--name",
        nargs="?",
        type=str,
        help="name to assign to the manager.",
        default=f"vine-blast-{getpass.getuser()}",
    )
    parser.add_argument(
        "--port",
        nargs="?",
        type=int,
        help="port for the manager to listen for connections. If 0, pick any available.",
        default=9123,
    )
    parser.add_argument(
        "--disable-peer-transfers",
        action="store_true",
        help="disable transfers among workers.",
        default=False,
    )
    parser.add_argument(
        "--max-concurrent-transfers",
        nargs="?",
        type=int,
        help="maximum number of concurrent peer transfers",
        default=3,
    )
    args = parser.parse_args()

    m = vine.Manager(port=args.port)
    m.set_name(args.name)

    if args.disable_peer_transfers:
        m.disable_peer_transfers()
    
    if args.max_concurrent_transfers:
        m.tune("worker-source-max-transfers", args.max_concurrent_transfers)

    print("Declaring files...")
    blast_url = m.declare_url(
        "https://ftp.ncbi.nlm.nih.gov/blast/executables/blast+/2.13.0/ncbi-blast-2.13.0+-x64-linux.tar.gz",
        cache="always",  # with "always", workers keep this file until they are terminated
    )
    blast = m.declare_untar(blast_url, cache="always")

    landmark_url = m.declare_url(
        "https://ftp.ncbi.nlm.nih.gov/blast/db/landmark.tar.gz",
        cache="always",
    )
    landmark = m.declare_untar(landmark_url)

    print("Declaring tasks...")
    for i in range(args.task_count):
        query = m.declare_buffer(make_query_text(args.query_count))
        t = vine.Task(
            command="blastdir/ncbi-blast-2.13.0+/bin/blastp -db landmark -query query.file",
            inputs={
                query: {"remote_name": "query.file"},
                blast: {"remote_name": "blastdir"},
                landmark: {"remote_name": "landmark"},
            },
            env={"BLASTDB": "landmark"},
            cores=1,
        )

        task_id = m.submit(t)
        print(f"submitted task {t.id}: {t.command}")

    print(f"TaskVine listening for workers on {m.port}")

    print("Waiting for tasks to complete...")
    while not m.empty():
        t = m.wait(5)
        if t:
            if t.successful():
                print(f"task {t.id} result: {t.std_output}")
            elif t.completed():
                print(
                    f"task {t.id} completed with an executin error, exit code {t.exit_code}"
                )
            else:
                print(f"task {t.id} failed with status {t.result}")

    print("all tasks complete!")
# vim: set sts=4 sw=4 ts=4 expandtab ft=python:
