#!/usr/bin/env python

# Copyright (C) 2022- The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

# This example shows some of the remote data handling features of taskvine.
# It performs an all-to-all comparison of twenty (relatively small) documents
# downloaded from the Gutenberg public archive.

# A small shell script (vine_example_guteberg_task.sh) is used to perform
# a simple text comparison of each pair of files.

import taskvine as vine
import sys

urls = [
    "http://www.gutenberg.org/files/1960/1960.txt",
    "http://www.gutenberg.org/files/1961/1961.txt",
    "http://www.gutenberg.org/files/1962/1962.txt",
    "http://www.gutenberg.org/files/1963/1963.txt",
    "http://www.gutenberg.org/files/1965/1965.txt",
    "http://www.gutenberg.org/files/1966/1966.txt",
    "http://www.gutenberg.org/files/1967/1967.txt",
    "http://www.gutenberg.org/files/1968/1968.txt",
    "http://www.gutenberg.org/files/1969/1969.txt",
    "http://www.gutenberg.org/files/1970/1970.txt",
    "http://www.gutenberg.org/files/1971/1971.txt",
    "http://www.gutenberg.org/files/1972/1972.txt",
    "http://www.gutenberg.org/files/1973/1973.txt",
    "http://www.gutenberg.org/files/1974/1974.txt",
    "http://www.gutenberg.org/files/1975/1975.txt",
    "http://www.gutenberg.org/files/1976/1976.txt",
    "http://www.gutenberg.org/files/1977/1977.txt",
    "http://www.gutenberg.org/files/1978/1978.txt",
    "http://www.gutenberg.org/files/1979/1979.txt",
    "http://www.gutenberg.org/files/1980/1980.txt",
    "http://www.gutenberg.org/files/1981/1981.txt",
    "http://www.gutenberg.org/files/1982/1982.txt",
    "http://www.gutenberg.org/files/1983/1983.txt",
    "http://www.gutenberg.org/files/1985/1985.txt",
    "http://www.gutenberg.org/files/1986/1986.txt",
    "http://www.gutenberg.org/files/1987/1987.txt",
]

url_count = 25

if __name__ == "__main__":
    m = vine.Manager()
    print("listening on port", m.port)

    for i in range(url_count):
        for j in range(url_count):
            t = vine.Task("./vine_example_gutenberg_script.sh filea.txt fileb.txt")

            t.add_input_file(
                "vine_example_gutenberg_script.sh",
                "vine_example_gutenberg_script.sh",
                cache=True,
            )
            t.add_input_url(urls[i], "filea.txt", cache=True)
            t.add_input_url(urls[j], "fileb.txt", cache=True)

            t.set_cores(1)

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
