#Copyright (C) 2020- The University of Notre Dame
#This software is distributed under the GNU General Public License.
#See the file COPYING for details.

import os
import socket
import json
from time import sleep
from subprocess import Popen

class WorkQueueServer:

    def __init__(self, workers):
        self.socket = socket.socket()
        self.id = 1
        self.num_workers = workers
        self.wq = None

    def send_recv(self, request):
        request = json.dumps(request)
        self.send(request)

        response = self.recv()

        self.id += 1

        return response

    def connect(self, address, server_port, wq_port):
        args = ['./work_queue_server', "%d" % server_port, "%d" % wq_port]#, '1>', '/dev/null', '2>&1']
        self.server = Popen(args)

        os.system("condor_submit_workers --cores 2 --memory 4000 --disk 10000 -M wq_bwa_json %d" % self.num_workers)

        i = 1
        while True:
            try:
                self.socket.connect((address, server_port))
                break
            except:
                sleep(0.1*i)
                i *= 2

    def send(self, msg):
        length = len(msg)

        total = 0
        sent = 0

        self.socket.send("%d" % length)

        while total < length:
            sent = self.socket.send(msg[sent:])
            if sent == 0:
                print("connection closed")
            total += sent

    def recv(self):
        response = self.socket.recv(4096)
        length = ''
        for t in response:
            if t != '{':
                length += t
            else:
                break

        response = response[len(length):]
        try:
            length = int(length)
            while len(response) < length:
                response += self.socket.recv(4096)
        except:
            pass
        
        return response

    def submit(self, task):
        request = {
            "jsonrpc" : "2.0",
            "method" : "submit",
            "id" : self.id,
            "params" : task
        }

        return self.send_recv(request)

    def wait(self, timeout):
        request = {
            "jsonrpc" : "2.0",
            "method" : "wait",
            "id" : self.id,
            "params" : timeout
        }

        return self.send_recv(request)

    def remove(self, taskid):
        request = {
            "jsonrpc" : "2.0",
            "method" : "remove",
            "id" : self.id,
            "params" : taskid
        }

        return self.send_recv(request)

    def disconnect(self):
        self.socket.close()
        Popen.terminate(self.server)

        os.system("condor_rm ccarball")

    def wq_empty(self):
        request = {
            "jsonrpc" : "2.0",
            "method" : "empty",
            "id" : self.id,
            "params" : ""
        }

        response = self.send_recv(request)
        response = json.loads(response)
        empty = response["result"]

        if empty == "Not Empty":
            return False
        else:
            return True
