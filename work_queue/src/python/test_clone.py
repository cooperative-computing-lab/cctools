import work_queue as wq

q = wq.WorkQueue(9123)

t0 = wq.Task('ls')
t1 = t0.clone()
t2 = t0.clone()
t3 = t1.clone()

for t in [t0,t1,t2,t3]:
    print 'Starting', t._task
    q.submit(t)

results = list()
while not q.empty():
    r = q.wait(5)
    if r:
        print 'Recieved', r._task
        results.append(r)

print 'Done!'
del results
