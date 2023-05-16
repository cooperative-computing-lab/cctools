#!/usr/bin/env python3

import os
import sys
import re
import operator
from collections import defaultdict
import json
from glob import glob


PATH1_RE = re.compile("</.+?>")
PATH2_RE = re.compile("<.+?>,")
PID1_RE = re.compile("\[pid [0-9]+\]")
BYTES_RE = re.compile("= [0-9]+$")
COMMAND_RE = re.compile('\[".+\]')
PROCESS_RE = re.compile("strace: Process [0-9]+")


ACTION_PRIORITY = {
    "?": 1,
    "SU": 2,
    "OU": 3,
    "M": 4,
    "S": 5,
    "R": 6,
    "W": 7,
    "WR": 8,
}


# Classes
class Properties:
    def __init__(
        self,
        path="",
        action="?",
        freq=0,
        read_freq=0,
        write_freq=0,
        sub_pid=[],
        read_size=0,
        write_size=0,
        size=0,
    ):
        self.path = path
        self.action = action
        self.freq = freq
        self.read_freq = read_freq
        self.read_size = read_size
        self.write_freq = write_freq
        self.write_size = write_size
        self.sub_pid = sub_pid.copy()
        self.size = size

    def convert_dict(self, path_dict):
        for key in path_dict:

            path_dict[key] = Properties(
                path=path_dict[key]["path"],
                action=path_dict[key]["action"],
                freq=path_dict[key]["freq"],
                read_freq=path_dict[key]["read_freq"],
                write_freq=path_dict[key]["write_freq"],
                sub_pid=path_dict[key]["sub_pid"],
                read_size=path_dict[key]["read_size"],
                write_size=path_dict[key]["write_size"],
                size=path_dict[key]["size"],
            )
        return path_dict


# Funtions
def usage():
    """prints the usage"""
    print(
        """
        filetrace can be called directly from the command line using:

            $ filetrace <command_to_excecute>

        filetrace also has optional command line flags:

            * -d <num>  : how many levels deep to summarise
            * -t <num>  : only show the top <num> of results on the summary page
            * --clean   : remove all file_trace files in the current directory
        """
    )
    sys.exit(0)


def create_trace_file(name, arg):
    """Runs strace and redirects its output to <name>.filetrace-1.txt"""
    arguments = " ".join(arg)
    exit_code = os.system(
        f"strace -f -y --trace=file,read,write,mmap {arguments} 2> {name}.filetrace-1.txt"
    )
    if exit_code:
        print(
            f"Program finished with exit code {exit_code} however, filetrace attempted to run"
        )


def create_dict(name):
    """creates path_dict which the key is the file path and the value is its properties:"""
    path_dict = {}
    subprocess_dict = {}
    call_counter = -1
    print()

    with open(name + ".filetrace-1.txt") as file:
        for line in file:
            call_counter += 1
            if "openat(" in line or "stat" in line:
                try:  # Try get file path
                    path = PATH1_RE.search(line).group(0).strip("<>")
                except AttributeError:  # AttributeError if file not found
                    try:  # find path for ENOENT
                        path = line.split('"')[1]
                    except IndexError:
                        continue
                path = os.path.realpath(path)  # make all paths absolute

                if path not in path_dict:
                    path_dict[path] = Properties(path=path)

                path_dict[path].action = openat_stat_actions(path_dict, path, line)
                path_dict[path].freq += 1

                if line.startswith("[pid "):  # checks is it is a subprocess
                    path_dict[path].sub_pid.append(
                        PID1_RE.search(line).group(0).strip("[pid ]")
                    )

            elif "read(" in line or "write(" in line:
                read_write_actions(path_dict, line)
            elif "mmap(" in line:
                try:
                    path = PATH2_RE.search(line).group(0).strip("<>,")
                    path = os.path.realpath(path)

                    if path not in path_dict:
                        path_dict[path] = Properties(path=path, action="M")
                except AttributeError:
                    pass

            if "strace: Process " in line:  # new process created
                find_pid(line, subprocess_dict)
            if "execve" in line:  # finds command associated with process
                find_command(line, subprocess_dict)
            
            if call_counter < 1000 or call_counter % 1000 == 0:
                print(f"filetrace: syscalls processed: {call_counter}", end="\r")

    return path_dict, subprocess_dict


def openat_stat_actions(path_dict, path, line):
    """Lablels the action for each path:
    A  : read but file not found
    R  : Read only
    W  : Write only
    RW : Read and write
    S  : stat
    """

    command = line

    if "openat" in command:
        if "ENOENT" in command:
            action = "OU"
        elif "RDONLY" in command:
            action = "M"
        elif "WRONLY" in command:
            action = "W"
        elif "RDWR" in command:
            action = "WR"
        else:
            action = "?"

    elif "stat" in command:
        if "ENOENT" in command:
            action = "SU"
        else:
            action = "S"

    old_action = path_dict[path].action
    if ACTION_PRIORITY[old_action] > ACTION_PRIORITY[action]:
        action = old_action
    return action


def read_write_actions(path_dict, line):
    try:
        path = PATH2_RE.search(line).group(0).strip("<>,")
        path = os.path.realpath(path)
        bytes_returned = int(BYTES_RE.search(line).group(0).replace("= ", ""))

    except (IndexError, AttributeError) as e:
        return 0

    if path not in path_dict:
        path_dict[path] = Properties(path=path)

    if "read(" in line:
        path_dict[path].action = "R"
        path_dict[path].read_freq += 1
        path_dict[path].read_size += bytes_returned
    elif "write(" in line:
        path_dict[path].action = "W"
        path_dict[path].write_freq += 1
        path_dict[path].write_size += bytes_returned
    if (path_dict[path].read_freq > 0) and (path_dict[path].write_freq > 0):
        path_dict[path].action = "WR"
    path_dict[path].size += bytes_returned
    path_dict[path].freq = path_dict[path].read_freq + path_dict[path].write_freq


def print_summary_2(path_dict, name, master):
    """Creates the file <name>.filetrace-2.txt which contains the freqency,
    action, and path of each entry
    """
    f = open(name + ".filetrace-2.txt", "w")
    f.write(f"action bytes freq path\n")

    for file in sorted(
        path_dict.values(),
        key=operator.attrgetter("action", "size", "freq"),
        reverse=True,
    ):
        action = file.action
        freq = file.freq
        size = file.size
        path = file.path

        f.write(f"{action:>4}{convert_bytes(size):>8} {freq:<5}  {path}\n")

    f.close()


def find_command(line, subprocess_dict):
    if PID1_RE.search(line):
        pid = PID1_RE.search(line).group(0).strip("[pid ]")
        try:
            command = (
                str(COMMAND_RE.search(line).group(0)).strip("[]").replace('", "', " ")
            )
            subprocess_dict[pid] = {"command": command, "files": set()}
        except AttributeError:
            pass
    return


def find_pid(line, subprocess_dict):
    if PROCESS_RE.search(line):
        pid = PROCESS_RE.search(line).group(0).replace("strace: Process ", "")
        subprocess_dict[pid] = {"command": "", "files": set()}


def print_subprocess_summary(subprocess_dict, name):
    """Creates the file <name>.filetrace-4.txt which contains the details of the subprocesses"""
    f = open(name + ".filetrace-4.txt", "w")
    f.write(f"Subproccesses: \n\n")

    for pid in subprocess_dict:
        command = subprocess_dict[pid]["command"]

        f.write(f"pid : {pid} : {command}\n")

        for file in subprocess_dict[pid]["files"]:
            f.write(f"  {pid}   {file.action:4}{file.path}\n")
        f.write("\n\n")

    f.close()


def find_major_directories(path_dict, subprocess_dict, top, dirLvl, name):
    """creates <name>.filetrace-3.txt which summarizes the most frequently accesed paths"""
    major_dict = defaultdict(lambda: [0, 0])
    reads_dict = defaultdict(lambda: [0, 0])
    writes_dict = defaultdict(lambda: [0, 0])

    f = open(name + ".filetrace-3.txt", "w")

    major_paths = find_common_path(path_dict.keys(), dirLvl)
    major_paths.insert(0, "/usr/lib64/")

    for path in path_dict:
        action = path_dict[path].action
        freq = path_dict[path].freq
        size = path_dict[path].size
        sub_pid = path_dict[path].sub_pid

        for short_path in major_paths:
            if short_path in path:
                break
        major_dict[short_path][0] += freq
        major_dict[short_path][1] += size

        if action == "R":
            reads_dict[short_path][0] += freq
            reads_dict[short_path][1] += size
        elif action == "W" or action == "WR":
            writes_dict[short_path][0] += freq
            writes_dict[short_path][1] += size

        if sub_pid:
            try:
                for pid in sub_pid:
                    if type(subprocess_dict[pid]["files"]) == set:
                        subprocess_dict[pid]["files"].add(path_dict[path])
                    else:
                        subprocess_dict[pid]["files"].append(path_dict[path])
            except KeyError:
                pass

    major_dict = dict(sorted(major_dict.items(), key=lambda x: x[1][1], reverse=True))
    reads_dict = dict(sorted(reads_dict.items(), key=lambda x: x[1][1], reverse=True))
    writes_dict = dict(sorted(writes_dict.items(), key=lambda x: x[1][1], reverse=True))

    f.write("\nMajor Directories\n\n")
    for index, path in enumerate(major_dict, 1):
        f.write(
            f"{convert_bytes(major_dict[path][1]):>8} {major_dict[path][0]:<5}  {path}\n"
        )
        if index == top:
            break

    f.write("\nMajor Reads\n\n")
    for index, path in enumerate(reads_dict, 1):
        f.write(
            f"{convert_bytes(reads_dict[path][1]):>8} {reads_dict[path][0]:<5}  {path}\n"
        )
        if index == top:
            break

    f.write("\nMajor Writes\n\n")
    for index, path in enumerate(writes_dict, 1):
        f.write(
            f"{convert_bytes(writes_dict[path][1]):>8} {writes_dict[path][0]:<5}  {path}\n"
        )
        if index == top:
            break

    f.close()


def find_common_path(path_list, dirLvl):
    path_list = sorted(path_list)
    prefixes = set(["/".join(path.split("/")[1:dirLvl]) for path in path_list])
    major_paths = []
    for prefix in prefixes:
        common_prefix = []
        for index, path in enumerate(path_list):
            # if path starts with the prefix add to the list and find the common path
            if path.startswith("/" + prefix):
                common_prefix.append(path_list.pop(index))
        try:
            short_path = os.path.commonpath(common_prefix)
        except ValueError:
            short_path = "/" + prefix
        if short_path == "/":  # exclude root as path
            continue
        major_paths.append(short_path)

    return sorted(major_paths, reverse=True)


def convert_bytes(num):
    if num > 1000000000:
        num = num / 1000000000
        num = str(round(num, 2)) + "G"
    elif num > 1000000:
        num = num / 1000000
        num = str(round(num, 2)) + "M"
    elif num > 1000:
        num = num / 1000
        num = str(round(num, 2)) + "K"
    return num


def end_of_execute(name):
    print("\n----- filetrace -----")
    print("filetrace completed\n\nCreated summaries:")
    if os.path.isfile(name + ".filetrace-1.txt"):
        print(f"{name}.filetrace-1.txt : output of strace")
    if os.path.isfile(name + ".filetrace-2.txt"):
        print(f"{name}.filetrace-2.txt : the action and frequency performed on each file")
    if os.path.isfile(name + ".filetrace-3.txt"):
        print(f"{name}.filetrace-3.txt : summary of all the actions")
    if os.path.isfile(name + ".filetrace-4.txt"):
        print(f"{name}.filetrace-4.txt : summary of files accessed by subprocesses")
        print("\n")
    else:
        print("There was an error creating the summary")
        sys.exit(1)


def create_json(name, path_dict, subprocess_dict):
    for key, value in path_dict.items():
        value = vars(value)
        path_dict[key] = value

    json_data = json.dumps(path_dict)
    f = open(f"filetrace-path-{name}.json", "w")
    f.write(json_data)
    f.close()
    print(f"\n\ncreated: filetrace-path-{name}.json")

    for key, value in subprocess_dict.items():
        subprocess_dict[key]["files"] = list(subprocess_dict[key]["files"])

    json_data = json.dumps(subprocess_dict)
    f = open(f"filetrace-process-{name}.json", "w")
    f.write(json_data)
    f.close()
    print(f"created: filetrace-process-{name}.json")


def load_path_json():
    files = glob("filetrace-path*.json")
    path_dict = json.load(open(files.pop(), "r"))
    x = Properties()

    for filename in files:
        json_dict = json.load(open(filename, "r"))
        for path, properties in json_dict.items():
            if path in path_dict:
                for property, value in properties.items():
                    if property == "action" and (
                        ACTION_PRIORITY[path_dict[path][property]]
                        < ACTION_PRIORITY[value]
                    ):
                        path_dict[path][property] = value
                    elif type(value) is int or type(value) is list:
                        path_dict[path][property] += value
            else:
                path_dict[path] = properties
    path_dict = x.convert_dict(path_dict)
    return path_dict


def load_process_json():
    files = glob("filetrace-process*.json")
    process_dict = json.load(open(files.pop(), "r"))

    for filename in files:
        json_dict = json.load(open(filename, "r"))
        for pid, properties in json_dict.items():
            process_dict[pid] = properties

    return process_dict


def remove_files():
    files = glob("*.filetrace-*.txt") + glob("filetrace-*.json")
    if not files:
        print("no files to remove.")
        return
    for file in files:
        print(f"removed: {file}")
    if not os.system(f"rm {' '.join(files)}"):
        print("\ndone ... removed filetrace files")
    else:
        print("error deleting files.")


# Main
def main():
    top = -1
    dirLvl = 5
    json = 0
    assemble = 0
    name = 0

    arguments = sys.argv[1:]

    while arguments and arguments[0].startswith("-"):
        arg = arguments.pop(0)
        if arg == "--clean":
            remove_files()

            sys.exit(0)
        elif arg == "-h" or arg == "--help":
            usage()
        elif arg == "-d":
            dirLvl = int(arguments.pop(0)) + 1
        elif arg == "-t" or arg == "-top":
            top = int(arguments.pop(0))
        elif arg == "-j" or arg == "--json":
            json = 1
        elif arg == "-a" or arg == "--assemble":
            assemble = 1
        elif arg == "-n" or arg == "--name":
            name = arguments.pop(0)
        else:
            continue

    if len(arguments) == 0:
        usage()

    if not name:
        name = arguments[0]

    if not assemble:
        create_trace_file(name, arguments[0:])
        path_dict, subprocess_dict = create_dict(name)
    else:
        path_dict = load_path_json()
        subprocess_dict = load_process_json()

    if json:
        create_json(name, path_dict, subprocess_dict)
        sys.exit(0)

    print_summary_2(path_dict, name, assemble)
    find_major_directories(path_dict, subprocess_dict, top, dirLvl, name)

    print_subprocess_summary(subprocess_dict, name)

    end_of_execute(name)

    sys.exit(0)


if __name__ == "__main__":
    main()
