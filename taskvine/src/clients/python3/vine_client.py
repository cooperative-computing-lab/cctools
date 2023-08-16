# Copyright (C) 2022- The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

# This is a work in progress and not ready for deployment yet.

# This is a demo of accessing the manager without SWIG.
# This class invokes the tool vine_api_proxy as a sub-process,
# and communicates with the sub-process using JSON RPCs.

# To be complete, this example must be extended to include the
# full manager API, and include a non-trivial application.

# The same style of communication could be used to build clients
# in other languages using the vine_api_proxy without SWIG.

import os
import json

from time import sleep
from subprocess import Popen, PIPE

class Client:

    def __init__(self):
        self.id = 1
        self.proxy = None

    def connect(self, manager_port, project_name):
        args = ['vine_api_proxy', '-p', "%d" % manager_port, '-N', project_name ]
        self.proxy = Popen(args, stdout=PIPE, stdin=PIPE)
        readyline = self.proxy.stdout.readline()
        # read back port value from readyline
        
    def disconnect(self):
        self.proxy.terminate()
        self.proxy.wait()

    def sendmsg(self, msg):
        self.proxy.stdin.write(("%d\n" % len(msg)).encode('utf-8'))
        self.proxy.stdin.write(msg.encode())
        self.proxy.stdin.flush()

    def recvmsg(self):
        line = self.proxy.stdout.readline()
        length = int(line)
        return self.proxy.stdout.read(length)

    def rpc(self, method, params):
        request = {
            "jsonrpc" : "2.0",
            "method" : method,
            "id" : self.id,
            "params" : params
        }

        request = json.dumps(request)
        self.sendmsg(request)
        response = self.recvmsg()
        self.id += 1
        jsonrpc = json.loads(response)
        return jsonrpc["result"]

    def submit(self, task):
        return self.rpc("submit",task)

    def wait(self, timeout):
        return self.rpc("wait",timeout)

    def remove(self, task_id):
        return self.rpc("remove",task_id)

    def status(self):
        return self.rpc("status",None)

    def empty(self):
        return self.rpc("empty",None)


print("Creating API proxy...")
client = Client()
print("Checking manager status...")
client.connect(0,"testproject")
print("Manager status:")
print(client.status())

# vim: set sts=4 sw=4 ts=4 expandtab ft=python:
