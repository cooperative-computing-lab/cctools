#!/usr/bin/env python3

import os
import sys
import re
import operator
from collections import defaultdict
    
path1_regex = re.compile('</.+>')
path2_regex = re.compile('<.+>,')
pid1_re = re.compile('\[pid [0-9]+\]')
bytes_re = re.compile('= [0-9]+$')


# Classes
class Properties:
    def __init__(self, path="",action = '?',freq = 0, command = "", read_freq = 0, write_freq = 0, sub_pid = [], read_size = 0, write_size = 0, size = 0):
        self.path = path
        self.action = action
        self.freq = freq
        self.command = command
        self.read_freq = read_freq
        self.read_size = read_size
        self.write_freq = write_freq
        self.write_size = write_size
        self.sub_pid = sub_pid.copy()
        self.size = size

# Funtions
def usage():
    """ prints the usage """
    print("""
        filetrace can be called directly from the command line using:

            $ filetrace <command_to_excecute>

        filetrace also has optional command line flags:

            * -d <num>  : how many levels deep to summarise
            * -t <num>  : only show the top <num> of results on the summary page
            * --clean   : remove all file_trace files in the current directory
        """)
    sys.exit(0)


def create_trace_file(arg):
    """ Runs strace and redirects its output to <name>.fout1.txt """
    arguments = ' '.join(arg)
    os.system(f'strace -f -y --trace=file,read,write,mmap {arguments} 2> {arg[0]}.fout1.txt')


def create_dict(name):
    """ creates path_dict which the key is the file path and the value is its properties: 
    """
    path_dict = {}
    subprocess_dict = {}

    with open(name + ".fout1.txt") as file:
        for line in file: 
            if "openat(" in line or "stat" in line:
                try: # Try get file path
                    path = path1_regex.search(line).group(0).strip('<>')
                except AttributeError: # AttributeError if file not found
                    try: # find path for ENOENT
                        path = line.split('"')[1]
                    except IndexError:
                        continue
                path = os.path.realpath(path) # make all paths absolute
                          
                if path not in path_dict: 
                        path_dict[path] = Properties(path=path)

                path_dict[path].command = line
                path_dict[path].action = openat_stat_actions(path_dict, path)
                path_dict[path].freq += 1

                if line.startswith('[pid '): # checks is it is a subprocess
                    path_dict[path].sub_pid.append(pid1_re.search(line).group(0).strip('[pid ]'))

            elif "read(" in line or "write(" in line:
                read_write_actions(path_dict,line)
            elif "mmap(" in line:
                try:
                    path = path2_regex.search(line).group(0).strip('<>,')
                    path = os.path.realpath(path)

                    if path not in path_dict: 
                        path_dict[path] = Properties(path=path, action='M')
                except AttributeError:
                    pass
                
            if "strace: Process " in line: # new process created
                find_pid(line, subprocess_dict)
            if "execve" in line: # finds command associated with process
                find_command(line,subprocess_dict) 

    return path_dict, subprocess_dict 


def openat_stat_actions(path_dict, path):
    """ Lablels the action for each path:
        A  : read but file not found
        R  : Read only
        W  : Write only
        RW : Read and write
        S  : stat
    """ 
    command = path_dict[path].command

    if "openat" in command:
        if "ENOENT" in command:
            action = 'OU'
        elif "RDONLY" in command:
            action = 'M'
        elif "WRONLY" in command:
            action = 'W'
        elif "RDWR" in command:
            action = 'WR'
        else:
            action = '?'

    elif "stat" in command:
        if "ENOENT" in command:
            action = 'SU'
        else:
            action = 'S'

    return action
   
def read_write_actions(path_dict, line):
    try:
        path = path2_regex.search(line).group(0).strip('<>,')
        path = os.path.realpath(path)
        bytes_returned = int(bytes_re.search(line).group(0).replace('= ',''))

    except (IndexError, AttributeError) as e:
        return 0

    if path not in path_dict: 
        path_dict[path] = Properties(path=path)

    if "read(" in line:
        path_dict[path].action = 'R'
        path_dict[path].read_freq += 1
        path_dict[path].read_size += bytes_returned
    elif "write(" in line:
        path_dict[path].action = 'W'
        path_dict[path].write_freq += 1
        path_dict[path].write_size += bytes_returned
    if (path_dict[path].read_freq > 0) and (path_dict[path].write_freq > 0):
        action = 'WR'
    path_dict[path].size += bytes_returned
    path_dict[path].freq = path_dict[path].read_freq + path_dict[path].write_freq


def print_summary_2(path_dict, name):
    """ Creates the file <name>.fout2.txt which contains the freqency,
    action, and path of each entry
    """
    f = open(name + ".fout2.txt", "w")
    f.write(f"action bytes freq path\n")
     
    for file in sorted(path_dict.values(), key=operator.attrgetter('action','size','freq') , reverse=True):
        action = file.action
        freq = file.freq
        size = file.size
        path = file.path
    
        f.write(f"{action:>4}{size:8}{freq:4}  {path}\n")

    f.close()

def find_command(line, subprocess_dict):
    command_re = re.compile('\[".+\]')
    if pid1_re.search(line): 
        pid = pid1_re.search(line).group(0).strip('[pid ]')
        try:
            command = str(command_re.search(line).group(0)).strip('[]').replace('", "',' ')
            subprocess_dict[pid] = {"command" : command, "files": set()}
        except AttributeError:
            pass
    return

def find_pid(line, subprocess_dict):
    process_re = re.compile('strace: Process [0-9]+')
    if process_re.search(line): 
        pid = process_re.search(line).group(0).replace('strace: Process ','')
        subprocess_dict[pid] = {"command" : "", "files": set()} 

def print_subprocess_summary(subprocess_dict, name):
    """ Creates the file <name>.fout4.txt which contains the details of the subprocesses
    """
    f = open(name + ".fout4.txt", "w")
    f.write(f"Subproccesses: \n\n")
     
    for pid in subprocess_dict:
        command = subprocess_dict[pid]['command']
    
        f.write(f"pid : {pid} : {command}\n")

        for file in subprocess_dict[pid]['files']:
            f.write(f'  {pid}   {file.action:4}{file.path}\n')
        f.write("\n\n")

    f.close()


def find_major_directories(path_dict, subprocess_dict, top, dirLvl, name):
    """ creates <name>.fout3.txt which summarizes the most frequently accesed paths """
    major_dict = defaultdict(lambda :[0,0])
    reads_dict = defaultdict(lambda :[0,0])
    writes_dict = defaultdict(lambda :[0,0])

    f = open(name + ".fout3.txt", "w")

    common = find_common_path(path_dict.keys(),dirLvl)
    common.insert(0,'/usr/lib64/')

    for path in path_dict:
        action = path_dict[path].action
        freq = path_dict[path].freq
        size = path_dict[path].size
        sub_pid = path_dict[path].sub_pid

        # short_path = '/'.join(path.split('/')[0:dirLvl])
        
        for short_path in common:
            if short_path in path:
                break
        major_dict[short_path][0] += freq
        major_dict[short_path][1] += size
        
        if action == 'R':
            reads_dict[short_path][0] += freq
            reads_dict[short_path][1] += size
        elif action == 'W' or action == 'WR':
            writes_dict[short_path][0] += freq
            writes_dict[short_path][1] += size

        if sub_pid:
            try:
                for pid in sub_pid:
                    subprocess_dict[pid]['files'].add(path_dict[path])
            except KeyError:
                pass
    
    major_dict = dict(sorted(major_dict.items(), key=lambda x:x[1][1], reverse=True))
    reads_dict = dict(sorted(reads_dict.items(), key=lambda x:x[1][1], reverse=True))
    writes_dict = dict(sorted(writes_dict.items(), key=lambda x:x[1][1], reverse=True))

    f.write("\nMajor Directories\n\n")
    for index, path in enumerate(major_dict,1):
        f.write(f"{convert_bytes(major_dict[path][1]):>8} {major_dict[path][0]:<5}  {path}\n")
        if index == top:
            break

    f.write("\nMajor Reads\n\n")
    for index, path in enumerate(reads_dict,1):
        f.write(f"{reads_dict[path][1]:6} {reads_dict[path][0]:2}  {path}\n")
        if index == top:
            break

    f.write("\nMajor Writes\n\n")
    for index, path in enumerate(writes_dict,1):
        f.write(f"{writes_dict[path][1]:6} {writes_dict[path][0]:2}  {path}\n")
        if index == top:
            break

    f.close()

def find_common_path(arr,dirLvl):
    arr = sorted(arr)
    prefixes = set(['/'.join(path.split('/')[1:dirLvl]) for path in arr])
    common = []
    for prefix in prefixes:
        a = []
        for index, path in enumerate(arr):
            if path.startswith('/'+ prefix):
                a.append(arr.pop(index))
        try:
            short_path = os.path.commonpath(a)
        except ValueError:
            short_path = '/'+ prefix
            common.append(short_path)
        if short_path == '/':
            continue
        common.append(short_path)
    
    common = sorted(common, reverse=True)
    return common

def convert_bytes(num):
    if num > 1000000:
        num = num / 1000000
        num = str(round(num,2)) + 'M'
    elif num > 1000:
        num = num / 1000
        num = str(round(num,2)) + 'K'
    return num
    
    


def end_of_execute(name):
    print("\n----- filetrace -----")
    print("filetrace completed\n\nCreated summaries:")

    print(f"{name}.fout1.txt : output of strace")
    print(f"{name}.fout2.txt : the action and frequency performed on each file")
    print(f"{name}.fout3.txt : summary of all the actions")
    print(f"{name}.fout4.txt : summary of files accessed by subprocesses")
    print("\n")


# Main
def main():
    top=-1
    dirLvl=5

    arguments = sys.argv[1:]

    while arguments and arguments[0].startswith('-'):
        arg = arguments.pop(0)
        if arg == "--clean":
            os.system("rm ./*fout*.txt")
            print("removed ftrace files")
            sys.exit(0)
        elif arg == '--help':
            usage()
        elif arg == '-h':
            usage()
        elif arg == '-d':
            dirLvl = (int(arguments.pop(0)) + 1)
        elif arg == '-t': 
            top = int(arguments.pop(0))
        else:
            continue

    if len(arguments) == 0:
        usage()
                
    name = arguments[0]

    create_trace_file(arguments[0:])
    path_dict, subprocess_dict = create_dict(name)
    print_summary_2(path_dict, name)
    find_major_directories(path_dict, subprocess_dict, top, dirLvl, name)

    print_subprocess_summary(subprocess_dict, name)

    end_of_execute(name)

    sys.exit(0)

if __name__ == '__main__':
    main()
