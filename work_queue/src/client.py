import sys
from json_server import WorkQueueServer

def main():

    q = WorkQueueServer()

    #connect to server
    q.connect('127.0.0.1', port)
  
    #read in tasks and submit requests
    f = open("tasks.txt", "r")

    tasks = 0
    for task in f.readlines():
        response = q.submit(task)
        print(response)

        tasks += 1

    #close file
    f.close()

    #submit wait requests
    for task in range(tasks):
        response = q.wait(10)
        print(response)

    #disconnect
    response  = q.disconnect()
    print(response)


if __name__ == "__main__":
    main()
