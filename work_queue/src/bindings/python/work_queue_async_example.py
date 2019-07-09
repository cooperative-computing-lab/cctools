import work_queue_async
from time import sleep

q = work_queue_async.WorkQueue(9123)

for i in range(0,5):
    t = work_queue_async.Task('sleep 2; /bin/date > out.{}'.format(i))
    t.specify_output_file('out.{}'.format(i))
    q.submit(t)

while not q.empty():
    t = q.receive()    # same as t.wait(0)
    if t:
        print('task {} done. status: {}'.format(t.id, t.return_status))
    else:
        print('doing some work at the master...')
        sleep(1)

