# Copyright (C) 2022 The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.
import sys
import os
import time
import re

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

import work_queue as wq
import threading
from queue import Queue
from utils import file_hash

class DirectoryMonitor():
    def __init__(self, path, scheduler):
        self.dir_state = {}
        self.path = path
        self.scheduler = scheduler
        self.__initialize_state()

    def __initialize_state(self):
        for entry in list(os.scandir(self.path)):
            self.dir_state[entry.inode()] = entry.name
            if re.search("-pre-", entry.name):
                self.scheduler.push(os.path.join(self.path, entry.name))
            elif not re.search("-post-", entry.name):
                event_path = self.__process_file(os.path.join(self.path, entry.name))
                self.dir_state[entry.inode()] = event_path
                self.scheduler.push(event_path)

    def monitor(self):
        for entry in list(os.scandir(self.path)):
            if not entry.inode() in self.dir_state:
                new_event_path = self.__process_file(os.path.join(self.path, entry.name))
                self.dir_state[entry.inode()] = new_event_path
                self.scheduler.push(new_event_path)

    def __process_file(self, event_path):
        # construct new filename
        filename = os.path.basename(event_path)
        h = file_hash(event_path)
        new_filename = filename + "-pre-" + h
        dirname = os.path.dirname(event_path)
        new_event_path = os.path.join(os.path.dirname(event_path), new_filename)
        # rename the file
        os.rename(event_path, new_event_path)
        return new_event_path
