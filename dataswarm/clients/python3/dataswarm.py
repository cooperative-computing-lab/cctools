#Copyright (C) 2020- The University of Notre Dame
#This software is distributed under the GNU General Public License.
#See the file COPYING for details.

import os
import socket
import json
from time import sleep

class DataSwarm:

    def __init__(self):
        self.socket = socket.socket()
        self.id = 1
        self.wq = None

    # Handle sending and receiving messages

    def send_recv(self, request):
        request = json.dumps(request)
        self.send(request)

        response = self.recv()

        self.id += 1

        return response

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

    # Handle connecting to and disconnecting from manager

    def connect(self, address, port):
        i = 1
        while True:
            try:
                self.socket.connect(address,port)
                break
            except:
                sleep(0.1*i)
                i *= 2

    def disconnect(self):
        self.socket.close()

    # Task methods

    # t is a task description in JSON
    def task_submit(self, t):
        request = {
            "method" : "task-submit",
            "id" : self.id,
            "params" : {
                "task" : t
            }
        }

        return self.send_recv(request)

    def task_delete(self, taskid):
        request = {
            "method" : "task-delete",
            "id" : self.id,
            "params" : {
                "task_id" : taskid
            }
        }

        return self.send_recv(request)

    def task_retrieve(self, taskid):
        request = {
            "method" : "task-retrieve",
            "id" : self.id,
            "params" : {
                "task_id" : taskid
            }
        }

        return self.send_recv(request)

    # File methods

    # f is a file description in JSON
    def file_submit(self, f):
        request = {
            "method" : "file-submit",
            "id" : self.id,
            "params" : {
                "description" : f
            }
        }

        return self.send_recv(request)

    def file_commit(self, fileid):
        request = {
            "method" : "file-commit",
            "id" : self.id,
            "params" : {
                "uuid" : fileid
            }
        }

        return self.send_recv(request)

    def file_delete(self, fileid):
        request = {
            "method" : "file-submit",
            "id" : self.id,
            "params" : {
                "uuid" : fileid
            }
        }

        return self.send_recv(request)

    def file_copy(self, fileid):
        request = {
            "method" : "file-submit",
            "id" : self.id,
            "params" : {
                "uuid" : fileid
            }
        }

        return self.send_recv(request)

    # Service methods

    # s is a service description in JSON
    def service_submit(self, s):
        request = {
            "method" : "service-submit",
            "id" : self.id,
            "params" : {
                "description" : s
            }
        }

        return self.send_recv(request)

    def service_delete(self, serviceid):
        request = {
            "method" : "service-delete",
            "id" : self.id,
            "params" : {
                "uuid" : serviceid
            }
        }

        return self.send_recv(request)

    # Project methods

    # p is a project description in JSON
    def project_create(self, p):
        request = {
            "method" : "project-create",
            "id" : self.id,
            "params" : {
                "description" : p
            }
        }

        return self.send_recv(request)

    def project_delete(self, projectid):
        request = {
            "method" : "project-delete",
            "id" : self.id,
            "params" : {
                "uuid" : projectid
            }
        }

        return self.send_recv(request)        

    # Other methods

    def wait(self, timeout):
        request = {
            "method" : "wait",
            "id" : self.id,
            "params" : {
                "timeout" : timeout
            }
        }

        return self.send_recv(request)

    def queue_empty(self):
        request = {
            "method" : "queue-empty",
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

    # uuid is the id of the desired item (file, task, service, or project) 
    # if no uuid is provided, give the status of everything (?)
    def status(self, uuid=None):

        request = {
            "method" : "status",
            "id" : self.id,
            "params" : {
                "uuid" : uuid
            }
        }

        return self.send_recv(request)