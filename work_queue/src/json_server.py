import socket

class WorkQueueServer:

    def __init__(self):
        self.socket = socket.socket()
        self.id = 1

    def connect(self, address, port):
        self.socket.connect(address, port)

    def submit(self, task):
        request = {
            "jsonrpc" : "2.0",
            "method" : "submit",
            "id" : self.id,
            "params" : task
        }

        request = json.dumps(request)
        self.socket.send(request.encode())

        response = self.socket.recv(1024)

        self.id += 1

        return response

    def wait(self, timeout):
        request = {
            "jsonrpc" : "2.0",
            "method" : "wait",
            "id" : self.id,
            "params" : timeout
        }

        request = json.dumps(request)
        self.socket.send(request.encode())

        response = self.socket.recv(1024)

        self.id += 1

        return response

    def remove(self, taskid):
        request = {
            "jsonrpc" : "2.0",
            "method" : "remove",
            "id" : self.id,
            "params" : taskid
        }

        request = json.dumps(request)
        self.socket.send(request.encode())

        response = self.socket.recv(1024)

        self.id += 1

        return response

    def disconnect(self):
        request = {
            "jsonrpc" : "2.0",
            "method" : "disconnect",
            "id" : self.id,
            "params" : None
        }

        request = json.dumps(request)
        self.socket.send(request.encode())

        response = self.socket.recv(1024)

        self.socket.close()

        return response


