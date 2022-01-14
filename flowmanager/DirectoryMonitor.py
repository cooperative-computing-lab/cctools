import sys
import os
import time

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

import work_queue as wq
import threading
from queue import Queue

class DirectoryMonitor():
        def __init__(self, path, event_handler):
                self.dir_state = {}
                self.path = path
                self.event_handler = event_handler
                self.__initialize_state()

        def __initialize_state(self):
                for entry in os.scandir(self.path):
                        self.dir_state[entry.inode()] = entry.name

        def monitor(self):
                for entry in os.scandir(self.path):
                        if not entry.inode() in self.dir_state:
                                self.event_handler.handle(os.path.join(self.path, entry.name))
                                self.dir_state[entry.inode()] = entry.name

def main():
        # start WQ
        # queue = wq.WorkQueue(name="blyons1-flowmanager", port=9125)
        # print("listening on port {}".format(queue.port))

        queue = Queue()
        dm = DirectoryMonitor("/scratch365/blyons1/flowmanager-input", queue)
        dm.monitor()

if __name__ == '__main__':
        main()
