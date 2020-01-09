#Copyright (C) 2020- The University of Notre Dame
#This software is distributed under the GNU General Public License.
#See the file COPYING for details.

import socket
import json

class WorkQueueServer:

    def __init__(self):
        self.socket = socket.socket()
        self.id = 1

    def send_recv(self, request):
        request = json.dumps(request)
        request += "\n"
        self.send(request)

        response = self.recv()

        self.id += 1

        return response

    def connect(self, address, port):
        self.socket.connect((address, port))

    def send(self, msg):
        length = len(msg)

        total = 0
        sent = 0
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
        length = int(length)

        while len(response) < length:
            response += self.socket.rec(4096)
        
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
        request = {
            "jsonrpc" : "2.0",
            "method" : "disconnect",
            "id" : self.id,
            "params" : None
        }

        response = self.send_recv(request)

        self.socket.close()

        return response


