import hashlib
import re

def file_hash(filename):
        m = hashlib.sha256()

        f = open(filename, 'rb')
        
        nread = f.read(1024)
        while nread:
                m.update(nread)
                nread = f.read(1024)
        
        f.close()
        return m.hexdigest()

# parses a filename that has been preprocessed to include the hash
def parse_filename(filename):
    matches = re.search("(.*)-.*-(.*)(\..*)?", filename)
    original_name = matches.group(1)
    filehash = matches.group(2)
    extension = matches.group(3) if matches.group(3) else ""
    return (original_name, filehash, extension)


