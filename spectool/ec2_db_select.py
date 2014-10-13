#!/usr/bin/python

import sqlite3
conn = sqlite3.connect('ec2.db')

c = conn.cursor()

for row in c.execute('SELECT * FROM cloud_table'):
	print row
# We can also close the connection if we are done with it.
# Just be sure any changes have been committed or they will be lost.
conn.close()
