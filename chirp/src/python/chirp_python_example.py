#!/usr/bin/env cctools_python
# CCTOOLS_PYTHON_VERSION 2.7 2.6 2.5 2.4

import sys
import os

try:
    import Chirp
except ImportError:
    print 'Could not find Chirp module. Please set PYTHONPATH accordingly.'
    sys.exit(1)


def write_some_file(filename='bar.txt'):
    message  = '''
    One makeflow to rule them all, One catalog to find them,
    One workqueue to bring them all and in the darkness bind them
    In the land of condor where the Shadows lie.

'''

    try:
        file = open(filename, 'w')
        file.write(message)
        file.close()
    except IOError, e:
        print "Could not open for writing: %s" % ', '.join(e.value)
        sys.exit(1)


if __name__ == '__main__':

    if(len(sys.argv) != 3):
        print "Usage: %s HOST:PORT TICKET" % sys.argv[0]
        sys.exit(1)

    hostport = sys.argv[1]
    ticket   = sys.argv[2]

    write_some_file()

    try:
        client = Chirp.Client(hostport,
                              authentication = ['ticket'],
                              tickets = [ticket],
                              timeout = 15,
                              debug = True)
    except Chirp.AuthenticationFailure, e:
        print "Could not authenticate using: %s" % ', '.join(e.value)
        sys.exit(1)


    try:
        print 'Chirp server sees me, ' + client.identity
        print client.listacl('/')
        print client.ls('/')
    except IOError, e:
        print "Could access path: %s" % e
        sys.exit(1)

    try:
        client.put('bar.txt', '/bar.txt')
        client.get('/bar.txt', 'foo.txt')
    except Chirp.TransferFailure, e:
        print "Could not transfer file: %s" % e
        sys.exit(1)

    try:
        print client.stat('/bar.txt')
    except IOError, e:
        print "Could access path at chirp server: %s" % e
        sys.exit(1)


    try:
        print 'checksum of bar.txt is ' + client.hash('/bar.txt')
    except IOError, e:
        print "Could not compute hash of file: %s" % e
        sys.exit(1)

    try:
        client.rm('/bar.txt')
        os.remove('bar.txt')
        os.remove('foo.txt')
    except IOError, e:
        print "Could not remove path: %s" % e
        sys.exit(1)

    sys.exit(0)

