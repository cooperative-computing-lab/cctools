#! /usr/bin/python
import os
import sys
import subprocess
from optparse import OptionParser
from datetime import datetime
from stat import *
import re

def main():
	#Problem: extension support incremental download

	with open("result.run") as file_run:
		for line in file_run:
			if line[:8] == 'INSTANCE':
				#line = line.rstrip()
				print line[9:19]

	with open("result.describe") as file_describe:
		for line in file_describe:
			str1 = 'PRIVATEIPADDRESS' 
			if line[:len(str1)] == str1:
#				print line
				index = line.find("ec2")
				print line[index:-1]
	sys.exit(0)



if __name__ == "__main__":
	main()

#set sts=4 sw=4 ts=4 expandtab ft=python
