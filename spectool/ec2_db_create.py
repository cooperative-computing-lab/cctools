#!/usr/bin/python

import sqlite3
conn = sqlite3.connect('ec2.db')

c = conn.cursor()

# Create table
# each dependency is stored in the archive as tar.gz format
c.execute('''CREATE TABLE cloud_table
             (platform text, kernel text, os text, image_id text, instance_id text, ip_addr text, user text)''')

c.execute("INSERT INTO cloud_table VALUES ('x86_64', '2.6.18', 'redhat5', 'ami-d76a29e7', 'i-0bc9d106', 'ec2-54-186-101-95.us-west-2.compute.amazonaws.com', 'root')") 


# Save (commit) the changes
conn.commit()

conn.close()
