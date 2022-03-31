import work_queue as wq
from utils import file_hash
import os 
class BasicEventHandler():
        def __init__(self, my_wq):
                self.wq = my_wq

        def handle(self, event_path):
                def wq_task(path):
                        f = open(path)
                        data = f.read()
                        f.close()
                        return (path, data)

                # if file does not exist then skip it
                if not os.path.exists(event_path):
                        return

                # construct new filename
                filename = os.path.basename(event_path)
                h = file_hash(event_path)
                new_filename = filename + "-pre-" + h
                dirname = os.path.dirname(event_path)
                new_event_path = os.path.join(os.path.dirname(event_path), new_filename)
                # rename the file
                os.rename(event_path, new_event_path)
                event_path = new_event_path

                # rename the file
                print("handling:", event_path)
                task = wq.PythonTask(wq_task, event_path)
                task.specify_input_file("BasicEventHandler.py", cache=True)
                task.specify_input_file("hash_utils.py", cache=True)
                task.specify_cores(1)
                self.wq.submit(task)
