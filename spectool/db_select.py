#! /usr/bin/python

import sqlite3
conn = sqlite3.connect('software.db')

c = conn.cursor()

for row in c.execute('SELECT * FROM sw_table'):
	print row
# We can also close the connection if we are done with it.
# Just be sure any changes have been committed or they will be lost.
conn.close()
