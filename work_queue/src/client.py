import sys
import socket

def main():

    #connect to server
    s = socket.socket()
    port = 2345
    s.connect(('127.0.0.1', port))
  
    #read in tasks and submit requests
    fs = open("tasks.txt", "r")
    for request in fs.readlines():

        s.send(request.encode())
        print(s.recv(1024))


    #submit X wait requests
    fw = open("waits.txt", "r")
    for request in fw.readlines():

        s.send(request.encode())
        print(s.recv(4096))

    #disconnect
    fs.close()
    fw.close()
    s.close()
    


if __name__ == "__main__":
    main()
