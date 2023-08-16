from vine_futures import ManagerFt, FutureTask, FutureTaskError

q = ManagerFt(port = 9123, local_worker = {'cores':1, 'memory':512, 'disk':10000})
#q = ManagerFt(port = 9123)

# without callbacks, append task to a list and then wait for them
tasks = []
for i in range(3):
    t = FutureTask('/bin/date')
    q.submit(t)
    tasks.append(t)

for t in tasks:
    print('task {} finished: {}'.format(t.id, t.result()))

# using callbacks:
# when future is done, it is given as an argument to function
def report_done(task):
    print('task {} finished. printing from callback: {}'.format(task.id, task.result()))
    # even we could add new tasks in a callback with future.queue.submit(...)

for i in range(3):
    t = FutureTask('/bin/date')
    q.submit(t)
    t.add_done_callback(report_done)

# wait for all tasks to be done
q.join()


# dealing with tasks with errors:

tasks_with_errors = []

# task with missing executable:
t = FutureTask('/bin/executable-that-does-not-exists')
q.submit(t)
tasks_with_errors.append(t)

# missing input file
t = FutureTask('/bin/date')
t.add_input_file('some-filename-that-does-not-exists')
q.submit(t)
tasks_with_errors.append(t)

# missing output file
t = FutureTask('/bin/date')
t.add_output_file('some-filename-that-was-not-generated')
q.submit(t)
tasks_with_errors.append(t)

for t in tasks_with_errors:
    try:
        t.result()
    except FutureTaskError as e:
        print('task {} finished: {}'.format(e.task.id, e))

# vim: set sts=4 sw=4 ts=4 expandtab ft=python:
