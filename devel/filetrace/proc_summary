#!/usr/env/python3

#
#       proc_summary.py
#   This consumes the output of strace invoked by:
#   
#           strace -y --trace=file,read,write,lseek
#

import sys
import os
import subprocess
from dataclasses import dataclass

@dataclass()
class read_op:
    pos: int
    count: int

@dataclass()
class stat_entry:
    stat_name: str
    stat_size: int
    num_stats: int

@dataclass()
class file_entry:
    path: str
    local_name: str
    num_open: int
    num_reads: int
    num_writes: int
    s_entry: stat_entry
    cursor_pos: int
    bytes_read: int
    bytes_written: int
    read_ops: list
    in_directory: str

file_table = {}
stat_table = {}

cwd = None

def parse_log(log):
    for l in log.readlines():
        line = l.split('(')
        syscall = line[0]

        # this is at end of file
        if '+' in syscall:
            continue
        
        if syscall not in "openatreadwritestatlseek":
            continue

        line_items = line[1].split(',')
        
        if syscall == "stat":
            if "ENOENT" in l:
                continue
            stat_name = line_items[0].strip('\"')
            stat_size = int(line_items[2].split('=')[1])

            if stat_name not in stat_table.keys():
                stat_table[stat_name] = stat_entry(stat_name, stat_size, 1)
            else:
                stat_table[stat_name].num_stats += 1

            continue

        pathname = line_items[0].split('<')[1].split('>')[0]
        
        if syscall == "openat":
            local_name = line_items[1].strip(' \"')
            global cwd
            cwd = pathname
            pathname += ('/' + local_name)

            # this was likely a library access
            if local_name[0] == '/':
                continue

            if pathname not in file_table.keys():
                s_entry = None
                if local_name in stat_table.keys():
                    s_entry = stat_table[local_name]
                directory = '/'.join(pathname.split('/')[:-1])
                entry = file_entry(pathname, local_name, 1, 0, 0, s_entry, 0, 0, 0, [], directory)
                file_table[pathname] = entry

            else:
                file_table[pathname].num_open += 1

            continue
        
        if pathname not in file_table.keys():
            continue

        elif syscall == "read":
            file_table[pathname].num_reads += 1
            bytes_read = int(line[-1].split('=')[-1].strip('\n'))
            file_table[pathname].read_ops.append(read_op(file_table[pathname].cursor_pos, bytes_read))
            file_table[pathname].bytes_read += bytes_read
            file_table[pathname].cursor_pos += bytes_read
            if file_table[pathname].s_entry and file_table[pathname].cursor_pos > file_table[pathname].s_entry.stat_size:
                file_table[pathname].cursor_pos = file_table[pathname].s_entry.stat_size

        elif syscall == "write":
            file_table[pathname].num_writes += 1
            bytes_written = int(line[-1].split('=')[-1].strip('\n'))
            file_table[pathname].bytes_written += bytes_written
            file_table[pathname].cursor_pos += bytes_written
            if file_table[pathname].s_entry and file_table[pathname].cursor_pos > file_table[pathname].s_entry.stat_size:
                file_table[pathname].cursor_pos = file_table[pathname].s_entry.stat_size

        elif syscall == "lseek":
            new_pos = int(line_items[-1].split('=')[-1].strip('\n '))
            file_table[pathname].cursor_pos = new_pos
    return

def squash_reads(read_ops, redundant=False):
    reads = {}
    redundant_bytes = 0
    
    # remove duplicates
    for op in read_ops:
        if op.pos in reads.keys():
            redundant_bytes += min(reads[op.pos], op.count)
            reads[op.pos] = max(reads[op.pos], op.count)
        else:
            reads[op.pos] = op.count
    
    if redundant:
        reads["redundant"] = redundant_bytes
    return reads


def make_sparse_file(f_entry):
    # only generate readonly inputs
    if f_entry.bytes_written > 0:
        return False

    # this is a directory
    if f_entry.bytes_read == 0:
        return False

    pathname = f_entry.path
    local_name = f_entry.local_name
    
    local_path_dirs = local_name.split('/')

    # rep directory structure, if any
    for d in range(len(local_path_dirs)-1):
        try:
            os.mkdir(f"{local_path_dirs[d]}")
        except FileExistsError:
            pass

    # create file and erase if already present
    subprocess.run(["touch", f"{local_name}"])
    subprocess.run(["truncate", "-s", f"0", f"{local_name}"])

    # reduce duplicate and overlapping reads
    reads = squash_reads(f_entry.read_ops)
    

    cursor = 0
    for s in read_starts:
        # how much space to fill before data
        gap = s - cursor
       
        #print(f"Writing from {s} to {s + reads[s]}. cursor {cursor} gap {gap}")

        # fill space
        #print(["truncate", "-s", f"+{gap}", f"{local_name}"])
        subprocess.run(["truncate", "-s", f"+{gap}", f"{local_name}"])

        # write real data
        #print(["dd", f"if={pathname}", f"bs={reads[s]}", f"skip={s}", "count=1", "iflag=skip_bytes", ">>", f"{local_name}"])
        subprocess.run([f"dd status=none if={pathname} bs={reads[s]} skip={s} count=1 iflag=skip_bytes >> {local_name}"], shell=True)

        cursor = s + reads[s]

    return True


def reduce_inputs():
    try:
        os.mkdir("reduced_outputs")
    except FileExistsError:
        pass

    os.chdir('reduced_outputs')

    for k,v in file_table.items():
        if make_sparse_file(v):
            print(f"Reduced file {v.local_name}")


def get_access_pattern(entry):
    rval = []

    read_ops = squash_reads(entry.read_ops, redundant=True)

    redundant_bytes = read_ops.pop("redundant")

    read_starts = list(read_ops.keys())
    read_starts.sort()
   
    sequential = True
    for s in range(len(read_starts)-1):
        if read_ops[read_starts[s]] != read_starts[s+1]:
            sequential = False
            break

    if sequential:
        rval.append("seq")
    else:
        rval.append("rand")

    bytes_read = entry.bytes_read - redundant_bytes

    if entry.bytes_written == 0:
        rval.append("ronly")
        if entry.s_entry and bytes_read < entry.s_entry.stat_size:
            rval.append("partial")
        else:
            rval.append("complete")

    if entry.bytes_read == 0 and entry.bytes_written > 0:
        rval.append("wronly")

    return rval

# convert ronly+wronly to rw etc...
def squash_access_patterns(access):
    access = list(set(access))
    
    if "ronly" in access and "wronly" in access:
        access.remove("ronly")
        access.remove("wronly")
        access.append("rw")

    if "seq" in access and "rand" in access:
        access.remove("seq")

    if "partial" in access and "complete" in access:
        access.remove("complete")

    return access

def print_summary_files():
    print("--- Process I/O Summary ---")
    for k,v in file_table.items():
        line_labels= ["Name:", "Read Bytes", "Write Bytes", "Access"]
        line_items = [v.local_name, v.bytes_read, v.bytes_written, get_access_pattern(v)]
        if v.s_entry:
            line_labels.append("Size")
            line_items.append(v.s_entry.stat_size)

        for i in range(len(line_items)):
            print(f"{line_labels[i]} {line_items[i]}\t",end='')


        print()

def print_summary_directories(level):
    directories = []
    for k,v in file_table.items():
        directories.append(v.in_directory)

    directories = set(directories)

    print("--- Process I/O Summary ---")

    for d in directories:
        bytes_read = 0
        bytes_written = 0

        access_patterns = []

        for v in file_table.values():
            if d == v.in_directory:
                bytes_read += v.bytes_read
                bytes_written += v.bytes_written
                access_patterns += get_access_pattern(v)
                
        
        access_patterns = squash_access_patterns(access_patterns)

        if bytes_written > 0 and "ronly" in access_patterns:
            access_patterns.remove("ronly")

        line_labels= ["Name:", "Read Bytes", "Write Bytes", "Access"]
        line_items = [d, bytes_read, bytes_written, access_patterns]
        
        for i in range(len(line_items)):
            print(f"{line_labels[i]} {line_items[i]}\t",end='')

        print()
        

def print_summary_yaml():
    directories = []
    for k,v in file_table.items():
        directories.append(v.in_directory)

    directories = set(directories)

    for d in directories:
        bytes_read = 0
        bytes_written = 0

        access_patterns = []

        for v in file_table.values():
            if d == v.in_directory:
                bytes_read += v.bytes_read
                bytes_written += v.bytes_written
                access_patterns += get_access_pattern(v)
                
        access_patterns = squash_access_patterns(access_patterns)

        if bytes_written > 0 and "ronly" in access_patterns:
            access_patterns.remove("ronly")

        # use local name
        d = d.replace(cwd, './')

        line_labels= ["Name:", "Read Bytes", "Write Bytes", "Access"]
        line_items = [d, bytes_read, bytes_written, access_patterns]
        
        print(line_items[0])

        for i in range(1, len(line_items)):
            print(f"\t{line_labels[i]}: {line_items[i]}\t")

        print()



if __name__ == "__main__":
    trace_file = sys.argv[1]
    f = open(trace_file,'r')

    parse_log(f)
    #print_summary_directories(1)
    #print_summary_files()
    print_summary_yaml()

    if "--reduce-inputs" in sys.argv:
        reduce_inputs()


