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

import ndcctools.taskvine as vine

test_dir = tempfile.mkdtemp(prefix="vine.test", dir=".")
input_name = "input.file"
exec_name = "exec.file"

error = False


def cleanup():
    shutil.rmtree(test_dir)


atexit.register(cleanup)


def report_task(task, expected_result, expected_exit_code, expected_outputs=None):
    global error
    if not task:
        error = True
        print("\nTask did not complete in expected time.")
    else:
        print("\nTask '{command}' report:".format(command=task.command))
        print("result: {r}".format(r=task.result))
        print("exit code: {exit_code}".format(exit_code=task.exit_code))
        if task.output:
            print("stderr:\n+++\n{stderr}---".format(stderr=task.output.encode("ascii", "replace")))
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


output_count = 0


def next_output_name():
    global output_count
    output_count += 1
    return "output_file." + str(output_count)


if __name__ == "__main__":
    parser = argparse.ArgumentParser("Test for taskvine python bindings.")
    parser.add_argument("port_file", help="File to write the port the queue is using.")
    parser.add_argument("--ssl_key", default=None, help="SSL key in pem format.")
    parser.add_argument("--ssl_cert", default=None, help="SSL certificate in pem format.")

    args = parser.parse_args()

    wait_time = 30

    with open(path.join(test_dir, input_name), "w") as f:
        f.write("hello world\n")

    shutil.copyfile("/bin/cat", path.join(test_dir, exec_name))
    os.chmod(path.join(test_dir, exec_name), stat.S_IRWXU)

    q = vine.Manager(port=0, ssl=(args.ssl_key, args.ssl_cert))

    exec_file = q.declare_file(path.join(test_dir, exec_name), cache=True)
    input_file = q.declare_file(path.join(test_dir, input_name), cache=True)

    with open(args.port_file, "w") as f:
        print("Writing port {port} to file {file}".format(port=q.port, file=args.port_file))
        f.write(str(q.port))

    # simple task
    # define a task, sending stderr to console, and stdout to output
    output_name = next_output_name()
    t = vine.Task(f"./{exec_name} {input_name} 2>&1 > {output_name}")
    t.add_input(exec_file, exec_name)
    t.add_input(input_file, input_name)
    output_file = q.declare_file(path.join(test_dir, output_name), cache=False)
    t.add_output(output_file, output_name)

    q.submit(t)
    t = q.wait(wait_time)
    report_task(t, "success", 0, [path.join(test_dir, output_name)])

    # same simple task, but now we send the directory as an input
    output_name = next_output_name()
    t = vine.Task(f"cd my_dir && ./{exec_name} {input_name} 2>&1 > {output_name}")
    in_dir = q.declare_file(test_dir, cache=True)
    t.add_input(exec_file, exec_name)
    t.add_input(in_dir, "my_dir")
    output_file = q.declare_file(path.join(test_dir, output_name), cache=False)
    t.add_output(output_file, path.join("my_dir", output_name))

    q.submit(t)
    t = q.wait(wait_time)
    report_task(t, "success", 0, [path.join(test_dir, output_name)])

    # we bring back the outputs from a directory:
    output_name = next_output_name()
    t = vine.Task(f"mkdir outs && ./{exec_name} {input_name} 2>&1 > outs/{output_name}")
    t.add_input(exec_file, exec_name)
    t.add_input(input_file, input_name)
    outs = q.declare_file(path.join(test_dir, "outs"), cache=False)
    t.add_output(outs, "outs")

    q.submit(t)
    t = q.wait(wait_time)
    report_task(t, "success", 0, [path.join(test_dir, "outs", output_name)])

    # Execute a task that only communicates through buffers:
    inbuf = q.declare_buffer(bytes("This is only a test!", "utf-8"))
    outbuf1 = q.declare_buffer()
    outbuf2 = q.declare_buffer()

    t = vine.Task("cp input.txt output1.txt && cp input.txt output2.txt")
    t.add_input(inbuf, "input.txt")
    t.add_output(outbuf1, "output1.txt")
    t.add_output(outbuf2, "output2.txt")
    q.submit(t)
    t = q.wait(wait_time)
    report_task(t, "success", 0)

    if outbuf1.contents() != inbuf.contents() or outbuf2.contents() != inbuf.contents():
        print("incorrect output:\nout1: {}\nout2: {}\n".format(outbuf1.contents(), outbuf2.contents()))
        sys.exit(1)
    else:
        print("buffer outputs match: {}".format(inbuf.contents()))

    # should fail because the 'executable' cannot be executed:
    t = vine.Task(f"./{input_name}".format(input=input_name))
    t.add_input(input_file, input_name)

    q.submit(t)
    t = q.wait(wait_time)
    report_task(t, "success", 126)

    # should fail because the 'executable' cannot be found:
    t = vine.Task("./notacommand")

    q.submit(t)
    t = q.wait(wait_time)
    report_task(t, "success", 127)

    # should fail because an input file does not exists:
    t = vine.Task("./notacommand")
    t.add_input(q.declare_file("notacommand"), "notacommand")

    q.submit(t)
    t = q.wait(wait_time)
    report_task(t, "input missing", -1)

    # should fail because an output file was not created:
    output_name = next_output_name()
    t = vine.Task(f"./{exec_name} {input_name} 2>&1")
    t.add_input(exec_file, exec_name)
    t.add_input(input_file, input_name)
    output_file = q.declare_file(path.join(test_dir, output_name), cache=False)
    t.add_output(output_file, output_name)

    q.submit(t)
    t = q.wait(wait_time)
    report_task(t, "output missing", 0)

    # should succeed in the alloted time
    t = vine.Task("/bin/sleep 1")
    t.set_time_max(10)
    q.submit(t)
    t = q.wait(wait_time)
    report_task(t, "success", 0)

    # should fail in the alloted time
    t = vine.Task("/bin/sleep 10")
    t.set_time_max(1)
    q.submit(t)
    t = q.wait(wait_time)
    report_task(t, "max wall time", 9)

    # should run in the alloted absolute time
    t = vine.Task("/bin/sleep 1")
    t.set_time_end((time.time() + 5) * 1e6)
    q.submit(t)
    t = q.wait(wait_time)
    report_task(t, "success", 0)

    # should fail in the alloted absolute time
    t = vine.Task("/bin/sleep 10")
    t.set_time_end((time.time() + 2) * 1e6)
    q.submit(t)
    t = q.wait(30)
    report_task(t, "max end time", 9)

    # Pull down data from a url and unpack it via a minitask.
    # Note that we use a local file url of a small tarball to test the mechanism without placing a load on the network.
    f = q.declare_untar(q.declare_url("file://{}/dummy.tar.gz".format(os.getcwd())))
    t = vine.Task("ls -lR cctools | wc -l")
    t.add_input(f, "cctools")
    q.submit(t)
    t = q.wait(wait_time)
    report_task(t, "success", 0)

    # Create an explicit minitask description to run curl
    minitask = vine.Task("curl https://www.nd.edu -o output")
    output_file = q.declare_file("output", cache=False)
    minitask.add_output(output_file, "output")
    intask = q.declare_minitask(minitask)

    # Now generate an input file from a shell command:
    t = vine.Task("wc -l infile")
    t.add_input(intask, "infile")
    q.submit(t)
    t = q.wait(wait_time)
    report_task(t, "success", 0)

    # second time should have it cached (though we can't tell from here)
    t = vine.Task("wc -l infile")
    t.add_input(intask, "infile")
    q.submit(t)
    t = q.wait(wait_time)
    report_task(t, "success", 0)

    # Now generate an input file from a shell command:
    t = vine.Task("wc -l infile")
    url = q.declare_url("https://www.nd.edu", cache=True)
    t.add_input(url, "infile")
    q.submit(t)
    t = q.wait(wait_time)
    report_task(t, "success", 0)

    # second time should have it cached (though we can't tell from here)
    t = vine.Task("wc -l infile")
    t.add_input(url, "infile")
    q.submit(t)
    t = q.wait(wait_time)
    report_task(t, "success", 0)

    # generate an invalid remote input file, should get an input missing error.
    t = vine.Task("wc -l infile")
    url = q.declare_url("https://pretty-sure-this-is-not-a-valid-url.com", "infile")
    t.add_input(url, "infile")
    q.submit(t)
    t = q.wait(wait_time)
    report_task(t, "input missing", 1)

    # create a temporary output file, and then fetch its contents manually.
    t = vine.Task("echo howdy > output")
    temp = q.declare_temp()
    t.add_output(temp,"output")
    q.submit(t)
    t = q.wait(wait_time)
    report_task(t, "success", 0)

    data = q.fetch_file(temp)
    if(data == "howdy\n"):
        print("correct data returned from temp file")
    else:
        print("INCORRECT data returned from temp file: {}".format(data))
        error = True
    
    if error:
        sys.exit(1)

    sys.exit(0)
# vim: set sts=4 sw=4 ts=4 expandtab ft=python:
