#! /usr/bin/python

import sqlite3
conn = sqlite3.connect('software.db')

c = conn.cursor()

# Create table
# each dependency is stored in the archive as tar.gz format
c.execute('''CREATE TABLE sw_table
             (name text, version text, platform text, store text, store_type text, type text, checksum text)''')

c.execute("INSERT INTO sw_table VALUES ('cmssw-cvmfs', 'CMSSW_5_3_11', 'slc5_amd64_gcc462', 'cvmfs:cms.cern.ch', 'description', 'software', '')")
c.execute("INSERT INTO sw_table VALUES ('python', 'redhat5', 'x86_64', 'https://www3.nd.edu/~ccl/research/data/hep-case-study/python-x86_64_redhat5.tar.gz', 'description', 'software', '')")
c.execute("INSERT INTO sw_table VALUES ('cctools', 'redhat5', 'x86_64', 'https://www3.nd.edu/~ccl/research/data/hep-case-study/cctools-x86_64-redhat5.tar.gz', 'url', 'software', '28c477f2a13c3b62c6af59dbe15d404d')")
c.execute("INSERT INTO sw_table VALUES ('cms-siteconf-local-cvmfs', '', '', 'https://www3.nd.edu/~ccl/research/data/hep-case-study/SITECONF.tar.gz', 'url', 'data', '2efd5cbb3424fe6b4a74294c84d0fb43')")
c.execute("INSERT INTO sw_table VALUES ('redhat', '5.10', 'x86_64', 'https://www3.nd.edu/~ccl/research/data/hep-case-study/redhat-5.10-x86_64.tar.gz', 'url', 'os', '62aa9bc37afe3f738052da5545832c80')")

#
#c.execute('''CREATE TABLE sw_table
#             (name text, version text, platform text, distro text, url text, checksum text)''')
## Insert a row of data
#c.execute("INSERT INTO sw_table VALUES ('git', '2.1.1', 'x86_64', 'redhat5', 'https://www3.nd.edu/~ccl/research/data/hep-case-study/git-2.1.1-x86_64-redhat5.tar.gz', 'af33460697cd24c6e7b0bbffe1d411ee')")
#
#
# Save (commit) the changes
conn.commit()

conn.close()
