#! /usr/bin/env python

# taskvine python binding tests
# tests for missing/recursive inputs/outputs.

import argparse
import atexit
import sys
import tempfile
import os
import os.path as path
import shutil
import stat
import time

import taskvine as vine

test_dir    = tempfile.mkdtemp(prefix='vine.test', dir=".")
input_file  = 'input.file'
exec_file   = 'exec.file'

def cleanup():
    shutil.rmtree(test_dir)
atexit.register(cleanup)

def report_task(task, expected_result, expected_exit_code, expected_outputs=None):
    error = False
    if not task:
        error = True
        print("\nTask did not complete in expected time.")
    else:
        print("\nTask '{command}' report:".format(command=task.command))
        print("result: {as_str} {as_int}".format(as_str=task.result_string, as_int=task.result))
        print("exit code: {exit_code}".format(exit_code=task.exit_code))
        if task.output:
            print("stderr:\n+++\n{stderr}---".format(stderr=task.output.encode('ascii','replace')))
        if task.result != expected_result:
            error = True
            print("Should have finished with result '{result}', but got '{real}'.".format(result=expected_result, real=task.result))
        elif task.exit_code != expected_exit_code:
            error = True
            print("Should have finished with exit_code {expected_exit_code}, but got {real}.".format(expected_exit_code=str(expected_exit_code), real=str(task.exit_code)))
        elif expected_outputs:
            for out in expected_outputs:
                if not path.isfile(out):
                    error = True
                    print("Should have created file {output} but did not.".format(output=out))
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

if __name__ == '__main__':
    parser = argparse.ArgumentParser("Test for taskvine python bindings.")
    parser.add_argument('port_file', help='File to write the port the queue is using.')
    parser.add_argument('--ssl_key', default=None, help='SSL key in pem format.')
    parser.add_argument('--ssl_cert', default=None, help='SSL certificate in pem format.')

    args = parser.parse_args()

    wait_time = 30 

    with open(path.join(test_dir, input_file), 'w') as f:
        f.write('hello world\n')

    shutil.copyfile('/bin/cat', path.join(test_dir, exec_file))
    os.chmod(path.join(test_dir, exec_file), stat.S_IRWXU)


    q = vine.Manager(port=0, ssl=(args.ssl_key, args.ssl_cert), debug_log="manager.log")

    with open(args.port_file, 'w') as f:
        print('Writing port {port} to file {file}'.format(port=q.port, file=args.port_file))
        f.write(str(q.port))

    # simple task
    # define a task, sending stderr to console, and stdout to output
    output = output_file()
    t = vine.Task("./{exe} {input} 2>&1 > {output}".format(exe=exec_file, input=input_file, output=output))
    t.add_input_file(path.join(test_dir, exec_file), exec_file)
    t.add_input_file(path.join(test_dir, input_file), input_file)
    t.add_output_file(path.join(test_dir, output), output)

    q.submit(t)
    t = q.wait(wait_time)
    report_task(t, vine.VINE_RESULT_SUCCESS, 0, [path.join(test_dir, output)])

    # same task as above, but testing resubmission on final state
    for i in range(3):
        q.submit(t)
        t = q.wait(5)
    report_task(t, vine.VINE_RESULT_SUCCESS, 0, [path.join(test_dir, output)])

    # same simple task, but now we send the directory as an input
    output = output_file()
    t = vine.Task("cd my_dir && ./{exe} {input} 2>&1 > {output}".format(exe=exec_file, input=input_file, output=output))
    t.add_input_file(test_dir, 'my_dir')
    t.add_output_file(path.join(test_dir, output), path.join('my_dir', output))

    q.submit(t)
    t = q.wait(wait_time)
    report_task(t, vine.VINE_RESULT_SUCCESS, 0, [path.join(test_dir, output)])


    # we bring back the outputs from a directory:
    output = output_file()
    t = vine.Task("mkdir outs && ./{exe} {input} 2>&1 > outs/{output}".format(exe=exec_file, input=input_file, output=output))
    t.add_input_file(path.join(test_dir, exec_file), exec_file)
    t.add_input_file(path.join(test_dir, input_file), input_file)
    t.add_output_file(path.join(test_dir, 'outs'), 'outs' )

    q.submit(t)
    t = q.wait(wait_time)
    report_task(t, vine.VINE_RESULT_SUCCESS, 0, [path.join(test_dir, 'outs', output)])

    # Execute a task that only communicates through buffers:

    original = "This is only a test!";
    t = vine.Task("cp input.txt output1.txt && cp input.txt output2.txt")
    t.add_input_buffer(original,"input.txt")
    t.add_output_buffer("out1","output1.txt")
    t.add_output_buffer("out2","output2.txt")
    q.submit(t)
    t = q.wait(wait_time)
    report_task(t, vine.VINE_RESULT_SUCCESS, 0)

    if t.get_output_buffer("out1") != original or t.get_output_buffer("out2") != original:
        print("incorrect output:\nout1: {}\nout2: {}\n".format(t.get_output_buffer("out1"),t.get_output_buffer("out2")))
        sys.exit(1)
    else:
        print("buffer outputs match the inputs.")



    # should fail because the 'executable' cannot be executed:
    t = vine.Task("./{input}".format(input=input_file))
    t.add_input_file(path.join(test_dir, input_file), input_file)

    q.submit(t)
    t = q.wait(wait_time)
    report_task(t, vine.VINE_RESULT_SUCCESS, 126)

    # should fail because the 'executable' cannot be found:
    t = vine.Task("./notacommand")

    q.submit(t)
    t = q.wait(wait_time)
    report_task(t, vine.VINE_RESULT_SUCCESS, 127)

    # should fail because an input file does not exists:
    t = vine.Task("./notacommand")
    t.add_input_file('notacommand')

    q.submit(t)
    t = q.wait(wait_time)
    report_task(t, vine.VINE_RESULT_INPUT_MISSING, -1)

    # should fail because an output file was not created:
    output = output_file()
    t = vine.Task("./{exe} {input} 2>&1".format(exe=exec_file, input=input_file))
    t.add_input_file(path.join(test_dir, exec_file), exec_file)
    t.add_input_file(path.join(test_dir, input_file), input_file)
    t.add_output_file(path.join(test_dir, output), output)

    q.submit(t)
    t = q.wait(wait_time)
    report_task(t, vine.VINE_RESULT_OUTPUT_MISSING, 0)

    # should succeed in the alloted time
    t = vine.Task("/bin/sleep 1")
    t.set_time_max(10)
    q.submit(t)
    t = q.wait(wait_time)
    report_task(t, vine.VINE_RESULT_SUCCESS, 0)

    # should fail in the alloted time
    t = vine.Task("/bin/sleep 10")
    t.set_time_max(1)
    q.submit(t)
    t = q.wait(wait_time)
    report_task(t, vine.VINE_RESULT_TASK_MAX_RUN_TIME, 9)

    # should run in the alloted absolute time
    t = vine.Task("/bin/sleep 1")
    t.set_time_end((time.time() + 5) * 1e6)
    q.submit(t)
    t = q.wait(wait_time)
    report_task(t, vine.VINE_RESULT_SUCCESS, 0)

    # should fail in the alloted absolute time
    t = vine.Task("/bin/sleep 10")
    t.set_time_end((time.time() + 2) * 1e6)
    q.submit(t)
    t = q.wait(30)
    report_task(t, vine.VINE_RESULT_TASK_TIMEOUT, 9)

    # Pull down data from a url and unpack it via a minitask.
    # Note that we use a local file url of a small tarball to test the mechanism without placing a load on the network.
    f = vine.FileUntar(vine.FileURL("file://dummy.tar.gz"))
    t = vine.Task("ls -lR cctools | wc -l")
    t.add_input(f,"cctools",cache=True)
    q.submit(t)
    t = q.wait(wait_time)
    report_task(t, vine.VINE_RESULT_SUCCESS, 0)
    
    # Create an explicit minitask description to run curl
    minitask = vine.Task("curl https://www.nd.edu -o output");
    minitask.add_output_file("output","output",cache=True);

    # Now generate an input file from a shell command:
    t = vine.Task("wc -l infile")
    t.add_input_mini_task(minitask,"infile",cache=True);
    q.submit(t)
    t = q.wait(wait_time)
    report_task(t, vine.VINE_RESULT_SUCCESS, 0)

    # second time should have it cached (though we can't tell from here)
    t = vine.Task("wc -l infile")
    t.add_input_mini_task(minitask,"infile",cache=True);
    q.submit(t)
    t = q.wait(wait_time)
    report_task(t, vine.VINE_RESULT_SUCCESS, 0)

    # Now generate an input file from a shell command:
    t = vine.Task("wc -l infile")
    t.add_input_url("https://www.nd.edu","infile",cache=True)
    q.submit(t)
    t = q.wait(wait_time)
    report_task(t, vine.VINE_RESULT_SUCCESS, 0)

    # second time should have it cached (though we can't tell from here)
    t = vine.Task("wc -l infile")
    t.add_input_url("https://www.nd.edu","infile",cache=True)
    q.submit(t)
    t = q.wait(wait_time)
    report_task(t, vine.VINE_RESULT_SUCCESS, 0)

    # generate an invalid remote input file, should get an input missing error.
    t = vine.Task("wc -l infile")
    t.add_input_url("https://pretty-sure-this-is-not-a-valid-url.com","infile",cache=True)
    q.submit(t)
    t = q.wait(wait_time)
    report_task(t, vine.VINE_RESULT_INPUT_MISSING, 1)

