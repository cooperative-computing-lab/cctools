#Copyright (C) 2020- The University of Notre Dame
#This software is distributed under the GNU General Public License.
#See the file COPYING for details.

import os
import socket
import json
import struct
from time import sleep

import logging

class DataSwarm:
    def __init__(self, host='127.0.0.1', port=1234, log_level=logging.DEBUG):
        self.id = 0

        self.log = self._setup_logging(log_level)

        self.header_spec = '!2sBxL'
        self.header_size = len(self._pack_header(0))

        self.socket = socket.socket()
        self.host = host
        self.port = int(port)

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()
        return False

    def _pack_header(self, size):
        return struct.pack(self.header_spec, b'MQ', 0b11, size)

    def _unpack_header(self, header):
        try:
            components = struct.unpack(self.header_spec, header);
        except struct.error as e:
            self.log.error("Message has a malformed header: {}".format(e))
            raise e
        return components[-1]

    def _setup_logging(self, log_level):
        log = logging.getLogger('DataSwarm')
        log.setLevel(log_level)
        ch = logging.StreamHandler()
        fm = logging.Formatter('%(asctime)s:%(name)s:%(levelname)s:%(message)s')
        ch.setFormatter(fm)
        log.addHandler(ch)
        return log

    # Handle sending and receiving messages
    def send(self, request):
        request["jsonrpc"] = "2.0"
        request = json.dumps(request)
        self.send_str(request)

    def send_recv(self, request):
        self.id += 1
        request["id"] = self.id
        self.send(request)
        response = self.recv()
        return response

    def send_str(self, msg):
        msg = msg.encode()
        size = len(msg)
        hdr = self._pack_header(size)
        self.socket.send(hdr)
        self.socket.send(msg)

    def recv(self):
        header = self.socket.recv(self.header_size)
        size   = self._unpack_header(header)
        response = self.socket.recv(size)
        response = json.loads(response)
        self.log.debug("rx: {}".format(response))

        return response

    # Handle connecting to and disconnecting from manager
    def connect(self):
        for i in range(0,10):
            last_exception = None
            try:
                self.log.debug("Connecting to {}:{}...".format(self.host, self.port))
                self.socket.connect((self.host,self.port))
                break
            except socket.timeout as e:
                self.log.debug("Connection timed-out!")
                sleep(0.1*(2**i)) # double wait time starting at 0.1 seconds
            except Exception as e:
                self.log.error("Timeout! Trying again in {} seconds.".format(wait))
                last_exception = e
        if last_exception:
            raise last_exception
        else:
            return self.handshake()

    def handshake(self):
        msg = {"method": "handshake",
               "params": { "type": "client" }}
        return self.send(msg)

    def disconnect(self):
        self.socket.close()

    # t is a task description in JSON
    def task_submit(self, t):
        request = {
            "method" : "task-submit",
            "params" : t
        }
        return self.send_recv(request)


    def task_delete(self, taskid):
        request = {
            "method" : "task-delete",
            "params" : {
                "task_id" : taskid
            }
        }

        return self.send_recv(request)


    def task_retrieve(self, taskid):
        request = {
            "method" : "task-retrieve",
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
            "params" : {
                "description" : f
            }
        }

        return self.send_recv(request)

    def file_commit(self, fileid):
        request = {
            "method" : "file-commit",
            "params" : {
                "uuid" : fileid
            }
        }

        return self.send_recv(request)

    def file_delete(self, fileid):
        request = {
            "method" : "file-delete",
            "params" : {
                "uuid" : fileid
            }
        }

        return self.send_recv(request)

    def file_copy(self, fileid):
        request = {
            "method" : "file-copy",
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
            "params" : {
                "description" : s
            }
        }

        return self.send_recv(request)

    def service_delete(self, serviceid):
        request = {
            "method" : "service-delete",
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
            "params" : {
                "description" : p
            }
        }

        return self.send_recv(request)

    def project_delete(self, projectid):
        request = {
            "method" : "project-delete",
            "params" : {
                "uuid" : projectid
            }
        }
        return self.send_recv(request)

    # Other methods
    def wait(self, timeout):
        request = {
            "method" : "wait",
            "params" : {
                "timeout" : timeout
            }
        }
        return self.send_recv(request)

    def queue_empty(self):
        request = {
            "method" : "queue-empty",
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
            "params" : {
                "uuid" : uuid
            }
        }

        return self.send_recv(request)
