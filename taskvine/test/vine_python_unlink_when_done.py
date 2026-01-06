#! /usr/bin/env python

import ndcctools.taskvine as vine
import os.path as path

filename = "hello.txt"


def create_file():
    with open(filename, "w") as f:
        f.write("hello")


m = vine.DaskVine(port=0)

print("testing without submitting task")
create_file()
assert path.exists(filename)
f = m.declare_file("hello.txt", unlink_when_done=True)
m.remove_file(f)
assert not path.exists(filename)


class FakeTask(vine.PythonTask):
    def __init__(self, filename):
        super().__init__(lambda x: x)
        self._filename = filename
        self._cleanup = None

    def submit_finalize(self):
        self._input = m.declare_file(self._filename, unlink_when_done=True)
        self.add_input(self._input, "input")
        super().submit_finalize()

    def __del__(self):
        self._manager.remove_file(self._input)
        super().__del__()


print("test unlink_when_done deleting task")
create_file()
assert path.exists(filename)

o = m.declare_temp()

m.log_debug_app("declaring task")
t = FakeTask(filename)

t.add_output(o, "output")

id = m.submit(t)
m.cancel_by_task_id(id)

t = m.wait(5)

assert path.exists(filename)
t = None
m.remove_file(o)
m.log_debug_app(f"was hello.txt deleted? {not path.exists(filename)}")

assert not path.exists(filename)

