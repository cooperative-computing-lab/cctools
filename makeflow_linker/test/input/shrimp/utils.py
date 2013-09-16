import gzip

def open_gz_or_ascii(file):
	fd = None

	try:
		fd = gzip.GzipFile(file)
	except IOError:
		fd = open(file, "r")

	try:
		line = fd.readline()
	except IOError:
		fd.close()
		fd = open(file, "r")

	fd.seek(0)

	return (fd)
