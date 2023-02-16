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

query = vine.FileBuffer("query",">P01013 GENE X PROTEIN (OVALBUMIN-RELATED)\nQIKDLLVSSSTDLDTTLVLVNAIYFKGMWKTAFNAEDTREMPFHVTKQESKPVQMMCMNNSFNVATLPAE\nKMKILELPFASGDLSMLVLLPDEVSDLERIEKTINFEKLTEWTNPNTMEKRRVKVYLPQMKIEEKYNLTS\nVLMALGMTDLFIPSANLTGISSAESLKISQAVHGAFMELSEDGIEMAGSTGVIEDIKHSPESEQFRADHP\nFLFLIKHNPTNTIVYFGRYWSP")
blast = vine.FileUntar(vine.FileURL("https://ftp.ncbi.nlm.nih.gov/blast/executables/blast+/LATEST/ncbi-blast-2.13.0+-x64-linux.tar.gz"))
landmark = vine.FileUntar(vine.FileURL("https://ftp.ncbi.nlm.nih.gov/blast/db/landmark.tar.gz"))

if __name__ == "__main__":
    m = vine.Manager()
    print("listening on port", m.port)

    m.set_scheduler(vine.VINE_SCHEDULE_FILES)

    for i in range(10):
        t = vine.Task("blastdir/ncbi-blast-2.13.0+/bin/blastp -db landmark -query query.file")

        t.add_input(query, "query.file", cache=True)
        t.add_input(blast, "blastdir", cache=True )
        t.add_input(landmark, "landmark", cache=True )
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
