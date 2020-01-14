#! /usr/bin/env python

# work queue python binding tests
# tests for missing/recursive inputs/outputs.

import atexit
import sys
import tempfile
import os
import os.path as path
import shutil
import stat

import work_queue as wq

test_dir    = tempfile.mkdtemp(prefix='wq.test', dir=".")
input_file  = 'input.file'
exec_file   = 'exec.file'

def cleanup():
    shutil.rmtree(test_dir)
atexit.register(cleanup)

def result_to_string(result):
    if result == wq.WORK_QUEUE_RESULT_SUCCESS:
        return 'success'

    if result == wq.WORK_QUEUE_RESULT_INPUT_MISSING:
        return 'input missing'

    if result == wq.WORK_QUEUE_RESULT_OUTPUT_MISSING:
        return 'output missing'

    if result == wq.WORK_QUEUE_RESULT_SIGNAL:
        return 'signal'

    if result == wq.WORK_QUEUE_RESULT_RESOURCE_EXHAUSTION:
        return 'resource exhaustion'

    if result == wq.WORK_QUEUE_RESULT_TASK_TIMEOUT:
        return 'resource exhaustion'

    if result == wq.WORK_QUEUE_RESULT_UNKNOWN:
        return 'unknown'

    if result == wq.WORK_QUEUE_RESULT_FORSAKEN:
        return 'forsaken'

    if result == wq.WORK_QUEUE_RESULT_MAX_RETRIES:
        return 'maximum retries'

    if result == wq.WORK_QUEUE_RESULT_TASK_MAX_RUN_TIME:
        return 'maximum runtime'

    if result == wq.WORK_QUEUE_RESULT_DISK_ALLOC_FULL:
        return 'disk allocation full'

    if result == wq.WORK_QUEUE_RESULT_RMONITOR_ERROR:
        return 'resource monitor error'

    return 'invalid result'

def report_task(task, expected_result, expected_exit_code, expected_outpus=None):
    error = False
    print("\nTask '{}' report:".format(t.command))
    if not task:
        error = True
        print("It was not completed by a worker.")
    else:
        print("result: {}".format(result_to_string(t.result)))
        print("exit code: {}".format(t.return_status))
        if t.output:
            print("stderr:\n+++\n{}---".format(t.output))
        if task.result != expected_result:
            error = True
            print("Should have finished with result '{}', but got '{}'.".format(result_to_string(expected_result), result_to_string(task.result)))
        elif task.return_status != expected_exit_code:
            error = True
            print("Should have finished with exit_code {}, but got {}.".format(str(expected_exit_code), str(task.return_status)))
        elif expected_outpus:
            for out in expected_outpus:
                if not path.isfile(out):
                    error = True
                    print("Should have created file {} but did not.".format(out))
        else:
            print("Completed as expected.")
    if error:
        sys.exit(1)


output_count = 0
def output_file():
    global output_count
    output_count += 1
    return 'output_file.' + str(output_count)

def make_task(exe, input, output):
    return t

port_file = None
try:
    port_file = sys.argv[1]
except IndexError:
    sys.stderr.write("Usage: {} PORTFILE\n".format(sys.argv[0]))


with open(path.join(test_dir, input_file), 'w') as f:
    f.write('hello world\n')

shutil.copyfile('/bin/cat', path.join(test_dir, exec_file))
os.chmod(path.join(test_dir, exec_file), stat.S_IRWXU)


q = wq.WorkQueue(0)

with open(port_file, 'w') as f:
    print('Writing port {} to file {}'.format(q.port, port_file))
    f.write(str(q.port))

# simple task
# define a task, sending stderr to console, and stdout to output
output = output_file()
t = wq.Task("./{} {} 2>&1 > {}".format(exec_file, input_file, output))
t.specify_input_file(path.join(test_dir, exec_file), exec_file)
t.specify_input_file(path.join(test_dir, input_file), input_file)
t.specify_output_file(path.join(test_dir, output), output)

q.submit(t)
t = q.wait(5)
report_task(t, wq.WORK_QUEUE_RESULT_SUCCESS, 0, [path.join(test_dir, output)])

# same simple task, but now we send the directory as an input
output = output_file()
t = wq.Task("cd {} && ./{} {} 2>&1 > {}".format('my_dir', exec_file, input_file, output))
t.specify_directory(test_dir, 'my_dir', recursive=True)
t.specify_output_file(path.join(test_dir, output), path.join('my_dir', output))

q.submit(t)
t = q.wait(5)
report_task(t, wq.WORK_QUEUE_RESULT_SUCCESS, 0, [path.join(test_dir, output)])


# we bring back the outputs from a directory:
output = output_file()
t = wq.Task("mkdir outs && ./{} {} 2>&1 > outs/{}".format(exec_file, input_file, output))
t.specify_input_file(path.join(test_dir, exec_file), exec_file)
t.specify_input_file(path.join(test_dir, input_file), input_file)
t.specify_directory(path.join(test_dir, 'outs'), 'outs', type = wq.WORK_QUEUE_OUTPUT)

q.submit(t)
t = q.wait(5)
report_task(t, wq.WORK_QUEUE_RESULT_SUCCESS, 0, [path.join(test_dir, 'outs', output)])

# should fail because the 'executable' cannot be executed:
t = wq.Task("./{}".format(input_file))
t.specify_input_file(path.join(test_dir, input_file), input_file)

q.submit(t)
t = q.wait(5)
report_task(t, wq.WORK_QUEUE_RESULT_SUCCESS, 126)

# should fail because the 'executable' cannot be found:
t = wq.Task("./notacommand")

q.submit(t)
t = q.wait(5)
report_task(t, wq.WORK_QUEUE_RESULT_SUCCESS, 127)

# should fail because an input file does not exists:
t = wq.Task("./notacommand")
t.specify_input_file('notacommand')

q.submit(t)
t = q.wait(5)
report_task(t, wq.WORK_QUEUE_RESULT_INPUT_MISSING, -1)

# should fail because an output file was not created:
output = output_file()
t = wq.Task("./{} {} 2>&1".format(exec_file, input_file))
t.specify_input_file(path.join(test_dir, exec_file), exec_file)
t.specify_input_file(path.join(test_dir, input_file), input_file)
t.specify_output_file(path.join(test_dir, output), output)

q.submit(t)
t = q.wait(5)
report_task(t, wq.WORK_QUEUE_RESULT_OUTPUT_MISSING, 0)


