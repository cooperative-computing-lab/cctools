#Copyright (C) 2020- The University of Notre Dame
#This software is distributed under the GNU General Public License.
#See the file COPYING for details.

import sys
import json
from json_server import WorkQueueServer

tasks = [ { "command_line" : "sed s/number/No./g file1.txt > file1.replaced" , "output_files" : [ { "local_name" : "file1.replaced" , "remote_name" : "file1.replaced" , "flags" : { "cache": True , "watch" : False } } ] , "input_files" : [ { "remote_name": "sed" , "local_name" : "/usr/bin/sed" , "flags" : { "cache" : False , "watch" : False } } , { "remote_name" : "file1.txt" , "local_name" : "file1.txt" , "flags" : { "cache" : False , "watch" : False } } ] }, { "command_line" : "sed s/number/No./g file2.txt > file2.replaced" , "output_files" : [ { "local_name" : "file2.replaced" , "remote_name" : "file2.replaced" , "flags" : { "cache": True , "watch" : False } } ] , "input_files" : [ { "remote_name": "sed" , "local_name" : "/usr/bin/sed" , "flags" : { "cache" : False , "watch" : False } } , { "remote_name" : "file2.txt" , "local_name" : "file2.txt" , "flags" : { "cache" : False , "watch" : False } } ] } , { "command_line" : "sed s/number/No./g file3.txt > file3.replaced" , "output_files" : [ { "local_name" : "file3.replaced" , "remote_name" : "file3.replaced" , "flags" : { "cache": True , "watch" : False } } ] , "input_files" : [ { "remote_name": "sed" , "local_name" : "/usr/bin/sed" , "flags" : { "cache" : False , "watch" : False } } , { "remote_name" : "file3.txt" , "local_name" : "file3.txt" , "flags" : { "cache" : False , "watch" : False } } ] } , { "command_line" : "sed s/number/No./g file4.txt > file4.replaced" , "output_files" : [ { "local_name" : "file4.replaced" , "remote_name" : "file4.replaced" , "flags" : { "cache": True , "watch" : False } } ] , "input_files" : [ { "remote_name": "sed" , "local_name" : "/usr/bin/sed" , "flags" : { "cache" : False , "watch" : False } } , { "remote_name" : "file4.txt" , "local_name" : "file4.txt" , "flags" : { "cache" : False , "watch" : False } } ] } , { "command_line" : "sed s/number/No./g file5.txt > file5.replaced" , "output_files" : [ { "local_name" : "file5.replaced" , "remote_name" : "file5.replaced" , "flags" : { "cache": True , "watch" : False } } ] , "input_files" : [ { "remote_name": "sed" , "local_name" : "/usr/bin/sed" , "flags" : { "cache" : False , "watch" : False } } , { "remote_name" : "file5.txt" , "local_name" : "file5.txt" , "flags" : { "cache" : False , "watch" : False } } ] } ]

def main():

    q = WorkQueueServer()

    #connect to server
    q.connect('127.0.0.1', 2345)
  
    #submit tasks
    for t in tasks:
        t = json.dumps(t)
        response = q.submit(t)
        print(response)
        
    #submit wait requests
    for _ in tasks:
        response = q.wait(10)
        print(response)

    #disconnect
    response  = q.disconnect()
    print(response)


if __name__ == "__main__":
    main()
