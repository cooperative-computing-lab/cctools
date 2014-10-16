#! /usr/bin/python

import sqlite3
conn = sqlite3.connect('software.db')

c = conn.cursor()

# Create table
# each dependency is stored in the archive as tar.gz format
c.execute('''CREATE TABLE sw_table
             (name text, version text, platform text, store text, store_type text, type text, checksum text)''')

c.execute("INSERT INTO sw_table VALUES ('cmssw-cvmfs', 'CMSSW_5_3_11', 'slc5_amd64_gcc462', 'cvmfs:cms.cern.ch', 'description', 'software', '')")
c.execute("INSERT INTO sw_table VALUES ('python', 'redhat5', 'x86_64', 'https://www3.nd.edu/~ccl/research/data/hep-case-study/python-x86_64-redhat5.tar.gz', 'description', 'software', 'd4734f811c263d79d5cb4bc89f07895c')")
c.execute("INSERT INTO sw_table VALUES ('git', 'redhat5', 'x86_64', 'https://www3.nd.edu/~ccl/research/data/hep-case-study/git-x86_64-redhat5.tar.gz', 'description', 'software', '62ceb4b5a715f252c599159eae40fc05')")
c.execute("INSERT INTO sw_table VALUES ('cctools', 'redhat5', 'x86_64', 'https://www3.nd.edu/~ccl/research/data/hep-case-study/cctools-x86_64-redhat5.tar.gz', 'url', 'software', '28c477f2a13c3b62c6af59dbe15d404d')")
c.execute("INSERT INTO sw_table VALUES ('cctools', 'centos5', 'x86_64', 'https://www3.nd.edu/~ccl/research/data/hep-case-study/cctools-x86_64-centos5.tar.gz', 'url', 'software', 'b4b67ca30d221a17abc88f776a9c4a08')")
c.execute("INSERT INTO sw_table VALUES ('cctools', 'redhat6', 'x86_64', 'https://www3.nd.edu/~ccl/research/data/hep-case-study/cctools-x86_64-redhat6.tar.gz', 'url', 'software', 'f7d1b8a9420dc233ac8c169a8f64c033')")
c.execute("INSERT INTO sw_table VALUES ('cctools', 'centos6', 'x86_64', 'https://www3.nd.edu/~ccl/research/data/hep-case-study/cctools-x86_64-centos6.tar.gz', 'url', 'software', 'ca3795099b624fdac7e63ce78ab4db8c')")
c.execute("INSERT INTO sw_table VALUES ('cctools', 'arch', 'x86_64', 'https://www3.nd.edu/~ccl/research/data/hep-case-study/cctools-x86_64-arch.tar.gz', 'url', 'software', '225402f948eb87a9e2d98e3a1ef79bc5')")
c.execute("INSERT INTO sw_table VALUES ('povray', 'redhat5', 'x86_64', 'https://www3.nd.edu/~ccl/research/data/hep-case-study/povray-x86_64-redhat5.tar.gz', 'url', 'software', '4d6025c8891231e9419296823af5d05f')")
c.execute("INSERT INTO sw_table VALUES ('cms-siteconf-local-cvmfs', '', '', 'https://www3.nd.edu/~ccl/research/data/hep-case-study/SITECONF.tar.gz', 'url', 'data', '2efd5cbb3424fe6b4a74294c84d0fb43')")
c.execute("INSERT INTO sw_table VALUES ('redhat', '5.10', 'x86_64', 'https://www3.nd.edu/~ccl/research/data/hep-case-study/redhat-5.10-x86_64.tar.gz', 'url', 'os', '62aa9bc37afe3f738052da5545832c80')")
c.execute("INSERT INTO sw_table VALUES ('redhat', '6.5', 'x86_64', 'https://www3.nd.edu/~ccl/research/data/hep-case-study/redhat-6.5-x86_64.tar.gz', 'url', 'os', '669ab5ef94af84d273f8f92a86b7907a')")
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
