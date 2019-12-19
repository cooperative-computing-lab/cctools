#! /usr/bin/env python3

# Francis Schickel
# Research for CCL at the University of Notre Dame
# Fall 2019

# Server Addr: /tmp/accelerator_socket

import socket
import sys
import os
import time
import array
import struct
import json
import argparse

##########################################################
#                                                        #
#   Function: send_fds()                                 #
#   Purpose: Client side function. Sends stdin, stdout,  #
#            and stderr FDs to server to dup2()          #
#                                                        #
##########################################################
def send_fds(sock, msg):
    client_in  = os.fdopen(os.dup(sys.stdin.fileno()), 'r')
    client_out = os.fdopen(os.dup(sys.stdout.fileno()), 'w')
    client_err = os.fdopen(os.dup(sys.stderr.fileno()), 'w')
    fds = [client_in.fileno(), client_out.fileno(), client_err.fileno()]
    sock.sendmsg([msg], [(socket.SOL_SOCKET, socket.SCM_RIGHTS, array.array("i", fds))])

##########################################################
#                                                        #
#   Function: recv_fds()                                 #
#   Purpose: Server side function. Recvs stdin, stdout,  #
#            and stderr FDs from client to dup2()        #
#                                                        #
##########################################################
def recv_fds(sock, msglen, maxfds):
    fds = array.array("i")   # Array of ints
    msg, ancdata, flags, addr = sock.recvmsg(msglen, socket.CMSG_LEN(maxfds * fds.itemsize))
    for cmsg_level, cmsg_type, cmsg_data in ancdata:
        if (cmsg_level == socket.SOL_SOCKET and cmsg_type == socket.SCM_RIGHTS):
            # Append data, ignoring any truncated integers at the end.
            fds.fromstring(cmsg_data[:len(cmsg_data) - (len(cmsg_data) % fds.itemsize)])
    return msg, list(fds)

##########################################################
#                                                        #
#   Function: copy_fds(msg, fds)                         #
#   Purpose: Server side function.                       #
#            The recv'ed fds and close all other FDs so  #
#            the client cannot access them               #
#                                                        #
##########################################################
def copy_fds(msg, fds):
    new_stdin  = os.fdopen(fds[0], 'r')
    new_stdout = os.fdopen(fds[1], 'w')
    new_stderr = os.fdopen(fds[2], 'w')
    os.dup2(fds[0], sys.stdin.fileno())
    os.dup2(fds[1], sys.stdout.fileno())
    os.dup2(fds[2], sys.stderr.fileno())
    for i in range(3, 21):
        try:
            os.close(i)
        except:
            continue


##########################################################
#                                                        #
#   Function: read_imports(FILE_NAME)                    #
#   Purpose: Server side function. Cycle through top of  #
#            file and exec all imports. Ignore comments  #
#            and whitespace. When non-import/comment/    #
#            whitespace is encountered, end loop         #
#                                                        #
##########################################################
def read_imports(file_name):
    source = open(file_name, "r")
    for line in source.readlines():
        line = line.rstrip()
        if line == "":
            continue
        words = line.split()
        if words[0][0] == '#':
            continue
        if words[0] != "from" and words[0] != "import":
            break
        exec(line)
    source.close()


##########################################################
#                                                        #
#   Function: recv_msg_from_client(sock_temp, sz)        #
#   Purpose: Receives msg of set size from set socket.   #
#                                                        #
##########################################################
def recv_msg_from_client(sock_temp, sz):
    amount_recved   = 0
    amount_expected = sz
    message = b''
    while amount_recved < amount_expected:
        data = sock_temp.recv(4096)
        amount_recved += sys.getsizeof(data)
        message = b''.join([message, data])
    return message

##########################################################
#                                                        #
#   Function: daemon_server()                            #
#   Purpose: Creates a server with a Unix Domain Socket  #
#            and will continually accept new procs       #
#                                                        #
#   TODO: Create timeout system                          #
#                                                        #
##########################################################
def daemon_server():
    server_address = '/tmp/accelerator_socket'
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.bind(server_address)
    sock.listen(1)
    while True:
        connection, client_address = sock.accept()
        try:
            # 1) Connect to proper FDs
            msg, fds = recv_fds(connection, 100, 4)
            # 2) Recv the expected message size through a packet
            message_size = recv_msg_from_client(connection, sys.getsizeof(struct.pack('!i', 1)))
            message_size = struct.unpack('!i', message_size)[0]
            # 3) Send ACK for receiving message size
            connection.sendall("ACK".encode())
            # 4) Recv message from client
            message_from_client = recv_msg_from_client(connection, message_size)
            message_from_client = json.loads(message_from_client.decode())
            read_imports(message_from_client['file'])
        except:
            connection.send(str.encode("ERR"))
            connection.close()
        # Fork child process to eval file
        pid = os.fork()
        if pid == 0:
            copy_fds(msg, fds)
            sys.argv = [message_from_client["file"]] 
            if message_from_client["args"]:
                sys.argv += message_from_client["args"]
            source = open(message_from_client['file'])
            read_source = source.read()
            # Convert file into an AST to eval
            compiled_source = compile(read_source, message_from_client['file'], 'exec')
            eval(compiled_source, globals(), locals())
            source.close()
            exit(0)
        else:
            childProcExit = os.waitpid(pid, 0)
        connection.send(str.encode("ACK"))
        connection.close()

##########################################################
#                                                        #
#   Function: fork_creation()                            #
#   Purpose: When first python proc is called, will need #
#            to spin up daemon server. This will fork    #
#            twice in order to daemonize the child proc  #
#            which will call the function: daemon_server #
#                                                        #
##########################################################
def fork_creation():
    pid = os.fork()
    if pid == 0:
        os.setsid()
        pid1 = os.fork()
        if pid1 == 0:
            daemon_server()
        else: exit(0)
    else:
        #TODO: change sleep time to smaller
        #atime.sleep(5)
        return

if __name__ == "__main__":
    # Parse args
    parser = argparse.ArgumentParser(description="Use this program to run multiple python processes and allow the modules to persist")
    parser.add_argument('--file', type=str, required=True, help="The file to be processed")
    parser.add_argument('--args', nargs="*", type=str, help="List of additional args")
    args = parser.parse_args()
    args_json = json.dumps(vars(args))
 
    # Create initial socket
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server_address = '/tmp/accelerator_socket'
    
    # Logic to check if server exists or not
    if os.path.exists(server_address):
        try:
            sock.connect(server_address)
        except: # Exception:
            sock.close()
            os.unlink(server_address)
            fork_creation()
            h = False
            #sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            #sock.connect(server_address)
            while not h:
                try:
                    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                    sock.connect(server_address)
                    h = True
                except:
                    sock.close()
    else:
        sock.close()
        fork_creation()
        h = False
        while not h:
            try:
                sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                sock.connect(server_address)
                h = True
            except:
                sock.close()
    # Server now exists and is connected to
    try:
        #send FDs to be copied by server
        send_fds(sock, str.encode("stdin, stdout, stderr"))
        # Send args
        message = str.encode(args_json)
        msg_sz = struct.pack('!i', sys.getsizeof(message))
        sock.send(msg_sz)
        first_ack = recv_msg_from_client(sock, sys.getsizeof("ACK".encode()))
        if first_ack.decode() != "ACK":
            print("Client: Failed to receive ACK from server for first message", file=sys.stderr)
            sock.close()
            exit(1)
        sock.sendall(message)

        second_ack = recv_msg_from_client(sock, sys.getsizeof("ACK".encode()))
        if second_ack.decode() != "ACK":
            print("Client: Failed to receive ACK from server for second message", file=sys.stderr)
            sock.close()
            exit(1)
    finally:
        sock.close()
