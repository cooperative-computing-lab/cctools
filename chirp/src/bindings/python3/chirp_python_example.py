#!/usr/bin/env python

import sys
import os

import ndcctools.chirp as chirp

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
    except IOError as e:
        print("Could not open for writing: %s" % ', '.join(e.value))
        sys.exit(1)


if __name__ == '__main__':

    if(len(sys.argv) != 3):
        print("Usage: %s HOST:PORT TICKET" % sys.argv[0])
        sys.exit(1)

    hostport = sys.argv[1]
    ticket   = sys.argv[2]

    write_some_file()

    try:
        client = chirp.Client(hostport,
                              authentication = ['ticket'],
                              tickets = [ticket],
                              timeout = 15,
                              debug = True)
    except chirp.AuthenticationFailure as e:
        print("Could not authenticate using: %s" % ', '.join(e.value))
        sys.exit(1)


    try:
        print('Chirp server sees me, ' + client.identity)
        print(client.listacl('/'))
        print(client.ls('/'))
    except IOError as e:
        print("Could access path: %s" % e)
        sys.exit(1)

    try:
        client.put('bar.txt', '/bar.txt')
        client.get('/bar.txt', 'foo.txt')
    except chirp.TransferFailure as e:
        print("Could not transfer file: %s" % e)
        sys.exit(1)

    try:
        print(client.stat('/bar.txt'))
    except IOError as e:
        print("Could access path at chirp server: %s" % e)
        sys.exit(1)


    try:
        print('checksum of bar.txt is ' + client.hash('/bar.txt'))
    except IOError as e:
        print("Could not compute hash of file: %s" % e)
        sys.exit(1)

    try:
        client.rm('/bar.txt')
        os.remove('bar.txt')
        os.remove('foo.txt')
    except IOError as e:
        print("Could not remove path: %s" % e)
        sys.exit(1)

    sys.exit(0)

