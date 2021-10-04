#!/usr/bin/env python3

import work_queue as wq

def wqmap(f, a):
    q = wq.WorkQueue(9123)
    results = []
    for i in a:
        p_task = wq.PythonTask(f, i)
        q.submit(p_task)

    while not q.empty():
        t = q.wait()
        if t:
            x = t.output
            if isinstance(x, wq.PythonTaskNoResult):
                print("Task {} failed and did not generate a result.".format(t.id))
            else:
                results.insert(0, x)

    return results

def wqallpairs(f, a, b):
    q = wq.WorkQueue(9123)
    results = []
    for i in a:
        for j in b:
            p_task = wq.PythonTask(f, i, j)
            q.submit(p_task)

    while not q.empty():
        t = q.wait()
        if t:
            x = t.output
            if isinstance(x, wq.PythonTaskNoResult):
                print("Task {} failed and did not generate a result".format(t.id))
            else:
                results.insert(0, x)

    return results

def wqtreereduce(f, a):
    q = wq.WorkQueue(9123)

    tmp = a
    size = len(a)

    while size != 1:
        results = []
        for i in range(len(tmp)//2):
            p_task = wq.PythonTask(f, tmp[(i*2)], tmp[(i*2)+1])
            q.submit(p_task)

        while not q.empty():
            t = q.wait()
            if t:
                x = t.output
                if isinstance(x, wq.PythonTaskNoResult):
                    print("Task {} failed and did not generate a result".format(t.id))
                else:
                    results.insert(0, x)
        size = size // 2
        tmp = results

    return tmp[0]

def muli(a):
    return a * 2

def pair(a, b):
    return a * b

def main():
    test = [1, 2, 3, 4, 8, 16, 32, 64, 128, 256, 512, 1024]
    a = [1, 2, 3, 4]
    b = [1, 2, 3, 4]
    c = [1, 2, 3, 4, 5, 6, 7, 8]
    print(test)
    results = wqmap(muli, test)
    print(results)

    print(a, b)
    results = wqallpairs(pair, a, b)
    print(results)


    print(c)
    results = wqtreereduce(pair, a)
    print(results)

if __name__ == "__main__":
    main() 
