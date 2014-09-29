#! /usr/bin/python2.7

import sqlite3
conn = sqlite3.connect('software.db')

c = conn.cursor()

# Create table
# each dependency is stored in the archive as tar.gz format
c.execute('''CREATE TABLE sw_table
             (name text, version text, platform text, distro text, url text, checksum text)''')

# Insert a row of data
c.execute("INSERT INTO sw_table VALUES ('git', '2.1.1', 'x86_64', 'redhat5', 'https://www3.nd.edu/~ccl/research/data/hep-case-study/git-2.1.1-x86_64-redhat5.tar.gz', 'af33460697cd24c6e7b0bbffe1d411ee')")

c.execute("INSERT INTO sw_table VALUES ('redhat', '5.10', 'x86_64', '', 'https://www3.nd.edu/~ccl/research/data/hep-case-study/redhat-5.10-x86_64.tar.gz', '62aa9bc37afe3f738052da5545832c80')")

# Save (commit) the changes
conn.commit()

conn.close()
