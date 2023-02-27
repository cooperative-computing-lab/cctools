#!/usr/bin/env python3

import os
import sys

# Funtions
def usage():
    """ prints the usage """
    print("""
        File trace can be called directly from the command line using:

            $ file_trace <command_to_excecute>

        File trace also has optional command line flags:

            * -d <num>  : how many levels deep to summarise
            * -t <num>  : only show the top <num> of results on the summary page
            * --clean   : remove all file_trace files in the current directory
        """)
    sys.exit(0)

def create_trace_file(arg):
    """ Runs strace and redirects its output to <name>.fout1.txt """
    arguments = ' '.join(arg)
    os.system(f'strace -f {arguments} 2> {arg[0]}.fout1.txt')


def create_dict(name):
    """ creates path_dict which the key is the file path and the value is its properties: 
        properties = [action, freq, size, parent, command]
    """
    path_dict = {}
    properties = ['?', 0, 0,"", ""]

    with open(name + ".fout1.txt") as file:
        for line in file:
            if "openat" in line:
                try:
                    path = line.split('"')[1]

                    properties[1] = path_dict.get(path, properties)[1] + 1
                    properties[4] = line

                    path_dict[path] = properties.copy()

                    properties[1] = 0
                except IndexError:
                    continue
            elif "stat" in line: # separate if statment becasue unexpected behavior is an or is used
                try:
                    path = line.split('"')[1]

                    properties[1] = path_dict.get(path, properties)[1] + 1
                    properties[4] = line

                    path_dict[path] = properties.copy()

                    properties[1] = 0
                except IndexError:
                    continue

    return path_dict 


def file_actions(path_dict, name):
    """ Lablels the action for each path:
        A  : read but file not found
        R  : Read only
        W  : Write only
        RW : Read and write
        S  : stat
    """
    for path, properties in path_dict.items():
        command = properties[4]

        if "openat" in command:
            if "ENOENT" in command:
                action = 'A'
            elif "RDONLY" in command:
                action = 'R'
            elif "WRONLY" in command:
                action = 'W'
            elif "RDWR" in command:
                action = 'RW'
            else:
                action = '?'

            properties[0] = action
            path_dict[path] = properties

        elif "stat" in command:
            try:
                action = 'S'
                properties[0] = action
                path_dict[path] = properties
            except IndexError:
                continue
     
    path_dict = dict(sorted(path_dict.items(), key=lambda x:x[1], reverse=True))

    return path_dict

    
def print_summary_2(path_dict, name):
    """ Creates the file <name>.fout2.txt which contains the freqency,
    action, and path of each entry
    """
    f = open(name + ".fout2.txt", "w")
    for path, properties in path_dict.items():
        action = properties[0]
        freq = properties[1]
    
        f.write(f" {freq:2}  {action:2}  {path}\n")
            
    f.close()


def find_major_directories(path_dict, top, dirLvl, name):
    """ creates <name>.fout3.txt which summarizes the most frequently accesed paths """
    count = 0

    f = open(name + ".fout3.txt", "w")
    f.write("\nMajor Directories\n\n")
    major_dict = {}
    reads_dict = {}
    writes_dict = {}

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

    for path, value in major_dict.items():
        f.write(f"{major_dict.get(path):2}  {path}\n")
        count += 1
        if count >= top:
            break

    count = 0

    f.write("\nMajor Reads\n\n")
    for path, value in reads_dict.items():
        f.write(f"{reads_dict.get(path):2}  {path}\n")
        count += 1
        if count >= top:
            break
    
    f.write("\nMajor Writes\n\n")
    for path, value in writes_dict.items():
        f.write(f"{writes_dict.get(path):2}  {path}\n")
        count += 1
        if count >= top:
            break

    f.close()

def end_of_execute(name):
    print("\n----- ftrace -----")
    print("ftrace completed\n\nCreated summaries:")

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
        match arg:
            case "--clean":
                os.system("rm ./*fout*.txt")
                print("removed ftrace files")
                sys.exit(0)
            case '--help':
                usage()
            case '-h':
                usage()
            case '-d':
                dirLvl = (int(arguments.pop(0)) + 1)
            case '-t': 
                top = int(arguments.pop(0))
            case other:
                pass
                
    name = arguments[0]

    create_trace_file(arguments[0:])
    path_dict = create_dict(name)
    path_dict = file_actions(path_dict, name)
    print_summary_2(path_dict, name)
    find_major_directories(path_dict,top,dirLvl,name)
    end_of_execute(name)

    sys.exit(0)

if __name__ == '__main__':
    main()
