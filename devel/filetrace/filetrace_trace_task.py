from dataclasses import dataclass
import graphlib
import sys

log = sys.argv[1]

file_table = {}

ignore_paths = ["/dev/", "/etc/", "/lib/", "pipe:", "/proc/", "/usr/"] 

@dataclass()
class table_entry:
    fname: str
    num_read: int
    num_write: int
    io: int
    timeidx: int


class dag_node:
    def __init__(self, outputs):
        self.outputs = outputs


def parse_log(f):
    dag = {}
    inputs = []
    outputs = []
    reset=0
    timeidx = 0
    for l in f.readlines():
        if "read(" in l:
            if reset:
                if len(outputs):
                    dag[dag_node(outputs)]=inputs
                inputs = []
                outputs = []
                reset = 0
            fname = l.split('<')[1].split('>')[0] 
            if any(p in fname for p in ignore_paths):
                continue
            if fname not in file_table.keys():
                inputs.append(fname)
                file_table[fname] = table_entry(fname, 1, 0, 0, timeidx)  
            else:
                inputs.append(fname)
                file_table[fname].num_read += 1
        elif "write(" in l:
            fname = l.split('<')[1].split('>')[0]
            if any(p in fname for p in ignore_paths):
                continue
            if fname not in file_table.keys():
                file_table[fname] = table_entry(fname, 0, 1, 1, timeidx)
            else:
                file_table[fname].num_write += 1
                file_table[fname].timeidx = timeidx
            outputs.append(fname)
            reset = 1
        timeidx += 1
    if len(inputs) and len(outputs):
        dag[dag_node(outputs)] = inputs

    return dag


def dedup(dag):
    result = {}
    delete = []
    for k in list(dag.keys()):
        if k not in dag:
            continue
        result[k] = dag[k]
        result[k] = set(result[k])
        for k2 in list(dag.keys()):
            if k == k2:
                continue
            if any(i in k2.outputs for i in k.outputs):
                result[k] = list(set(dag[k] + dag[k2]))
                del dag[k2]
    return result


def create_makefile(tasks):
    for k, v in tasks.items():
        reduced = [n.split('/')[-1] for n in k.outputs]
        print(str(reduced) + ":", end=" ")
        for f in v:
            print(str(f.split('/')[-1]), end=" ")
        print('\n\n')
    

if __name__ == "__main__":
    tasks_init = parse_log(open(log, 'r'))

    tasks = dedup(tasks_init)

    #for k, v in tasks.items():
    #    print("---- Outputs ----")
    #    if len(k.outputs) > 10:
    #        for i in k.outputs[:10]:
    #            print(i)
    #        for i in range(3):
    #            print('.')
    #    else:
    #        for i in k.outputs:
    #            print(i)
    #    print("\t---- Inputs ----")
    #    if len(v) > 10:
    #        for i in v[:10]:
    #            print(f'\t{i}')
    #        for i in range(3):
    #            print("\t.")
    #    else:
    #        for i in v:
    #            print(f'\t{i}')

    create_makefile(tasks)
