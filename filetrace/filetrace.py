#!/usr/bin/env python3

import os
import sys
import re

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
    os.system(f'strace -f -y --trace=file,read,write {arguments} 2> {arg[0]}.fout1.txt')


def create_dict(name):
    """ creates path_dict which the key is the file path and the value is its properties: 
        properties = [action, freq, size, parent, command, reads, writes]
    """
    path_dict = {}

    with open(name + ".fout1.txt") as file:
        for line in file:
            properties = ['?', 0, 0,"", "", 0 , 0]
            if "openat" in line or "stat" in line:
                try: # Try get file path
                    path = re.search('</.+>',line).group(0).replace('<','').replace('>','')
                except AttributeError: # AttributeError if file not found
                    try: # find path for ENOENT
                        path = line.split('"')[1]
                    except IndexError:
                        continue
                properties = path_dict.get(path, properties)
                properties[1] += 1
                properties[4] = line
                properties = file_actions(properties)
                path_dict[path] = properties.copy()

            if "read" in line or "write" in line:
                try:
                    path = re.search('<.+>,',line).group(0).replace('<','').replace('>,','')
                    bytes_written = re.search('= [0-9]+$',line).group(0).replace('= ','')
                    properties = path_dict.get(path, properties)
                    properties[2] = properties[2] + int(bytes_written)
                    path_dict[path] = properties.copy()
                except (IndexError, AttributeError) as e:
                    continue

    path_dict = dict(sorted(path_dict.items(), key=lambda x:x[1], reverse=True))

    return path_dict 


def file_actions(properties):
    """ Lablels the action for each path:
        A  : read but file not found
        R  : Read only
        W  : Write only
        RW : Read and write
        S  : stat
    """
    command = properties[4]

    if "openat" in command:
        if "ENOENT" in command:
            action = 'A'
        elif "RDONLY" in command:
            action = 'R'
            properties[5] += 1
        elif "WRONLY" in command:
            action = 'W'
            properties[6] += 1
        elif "RDWR" in command:
            action = 'WR'
            properties[5] += 1
            properties[6] += 1
        else:
            action = '?'
        if (properties[5] > 0) and (properties[6] > 0):
            action = 'WR'

    elif "stat" in command:
        action = 'S'
    
    properties[0] = action

    return properties
    
    
def print_summary_2(path_dict, name):
    """ Creates the file <name>.fout2.txt which contains the freqency,
    action, and path of each entry
    """
    f = open(name + ".fout2.txt", "w")
    f.write(f"freq action bytes path\n")
    for path, properties in path_dict.items():
        action = properties[0]
        freq = properties[1]
        size = properties[2]
    
        f.write(f"{freq:4}    {action:2}{size:8} {path}\n")
            
    f.close()


def find_major_directories(path_dict, top, dirLvl, name):
    """ creates <name>.fout3.txt which summarizes the most frequently accesed paths """
    count = 0
    major_dict = {}
    reads_dict = {}
    writes_dict = {}

    f = open(name + ".fout3.txt", "w")
    f.write("\nMajor Directories\n\n")

    for path, properties in path_dict.items():
        action = properties[0]
        freq = properties[1]

        short_path = '/'.join(path.split('/')[0:dirLvl])
        major_dict[short_path] = major_dict.get(short_path, 0) + freq
        
        if action == 'R':
            reads_dict[short_path] = major_dict.get(short_path)
        elif action == 'W':
            writes_dict[short_path] = major_dict.get(short_path)
    
    major_dict = dict(sorted(major_dict.items(), key=lambda x:x[1], reverse=True))
    reads_dict = dict(sorted(reads_dict.items(), key=lambda x:x[1], reverse=True))
    writes_dict = dict(sorted(writes_dict.items(), key=lambda x:x[1], reverse=True))

    for path in major_dict:
        f.write(f"{major_dict[path]:2}  {path}\n")
        count += 1
        if count >= top:
            break
    count = 0

    f.write("\nMajor Reads\n\n")
    for path in reads_dict:
        f.write(f"{reads_dict[path]:2}  {path}\n")
        count += 1
        if count >= top:
            break

    count = 0
    f.write("\nMajor Writes\n\n")
    for path in writes_dict:
        f.write(f"{writes_dict[path]:2}  {path}\n")
        count += 1
        if count >= top:
            break
            
    f.close()


def end_of_execute(name):
    print("\n----- filetrace -----")
    print("filetrace completed\n\nCreated summaries:")

    print(f"{name}.fout1.txt : output of strace")
    print(f"{name}.fout2.txt : the action and frequency performed on each file")
    print(f"{name}.fout3.txt : summary of all the actions")
    print("\n")


# Main

def main():
    top=5
    dirLvl=6

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
    path_dict = create_dict(name)
    print_summary_2(path_dict, name)
    find_major_directories(path_dict,top,dirLvl,name)
    end_of_execute(name)

    sys.exit(0)

if __name__ == '__main__':
    main()
