import sys
import os
import select
import time
import ndcctools.taskvine as vine
import pickle
import uuid
from multiprocessing import Pipe
from multiprocessing.connection import wait

class Chain():
    """
    A chain is group of tasks or groups that are executed to be executed sequentially.
    by default, running a singular group or seed eventually executes chain(group(seed))
    """
    def __init__ (self, *args):
        self._chain = []
        self._current_link = None
        self._current_tasks = {}
        self._pending_chains = {}
        self._pending_chain_tasks = {}
        self._waiting_tasks = {}
        self._managers = {}
        self._items = {}
        self._manager_links = []
        self._default_mgr = str(uuid.uuid1())
        self._item_id = str(uuid.uuid1())
        self._chain_id = None
        for arg in iter(args):
            try:
                iargs = iter(arg)
                for iarg in iargs:
                    if isinstance(iarg, Group):
                        self._chain.append(iarg)
                    elif isinstance(iarg, Seed):
                        self._chain.append(iarg)
                    else:
                        raise TypeError
            except:
                if isinstance(arg, Group):
                    self._chain.append(arg)
                elif isinstance(arg, Seed):
                    self._chain.append(arg)
                else:
                    raise TypeError

    def run(self):
        
        if self._chain:
            self._current_link = self._chain.pop(0)
        else:
            self._current_link = None
        while self._current_link:
            link = self._current_link
            # Move items in group to current task list.
            if isinstance(link, Group):
                self.exec_group(link)
                        
            # This is simple, execute task then grab next link
            elif isinstance(link, Seed):
                self.exec_single_seed(link)
            
            if self._chain:
                self._current_link = self._chain.pop(0)
            else:
                self._current_link = None

        print("Run Complete...!")

    def exec_group(self, group):
        for item in group._group:
            self._current_tasks[item._item_id] = item
        # Exceute current link as a group.
        while self._current_tasks or self._waiting_tasks: 
            # Queue current tasks
            for item in list(self._current_tasks.values()):
                if isinstance(item, Seed):
                    if not item._manager:
                        manager = self._default_mgr
                    else:
                        manager = item._manager
                    if manager not in self._managers:
                        read, write = run_manager(manager)
                        self._managers[manager] = {"read":read, "write":write}
                        self._manager_links.append(read)
                    
                    self._managers[manager]["write"].send(item)
                    self._waiting_tasks[item._item_id] = item
                    del self._current_tasks[item._item_id]
                    
                elif isinstance(item, Chain):
                    if item._chain:
                        chain_link = item._chain.pop(0)
                    else:
                        chain_link = None
                        if item._chain_id:
                            self.unlink_from_chain(chain)
                    if isinstance(chain_link, Seed):
                        self._current_tasks[chain_link._item_id] = chain_link
                        self._pending_chain_tasks[item._item_id] = {chain_link._item_id}
                        self._pending_chains[item._item_id] = item
                        chain_link._chain_id = item._item_id
                        del self._current_tasks[item._item_id]
                    elif isinstance(chain_link, Group):
                        for group_item in chain_link._group:
                            self._current_tasks[group_item._item_id] = group_item
                            if item._item_id not in self._pending_chain_tasks:
                                self._pending_chain_tasks[item._item_id] = set()
                            self._pending_chain_tasks[item._item_id].add(group_item._item_id)
                            group_item._chain_id = item._item_id
                        self._pending_chains[item._item_id] = item
                        del self._current_tasks[item._item_id]
                    else:
                        del self._current_tasks[item._item_id]
                else:
                    del self._current_tasks[item._item_id]

            # check for results from managers.
            self.check_results() 
    
    def exec_single_seed(self, seed):
        grouped_seed = Group(seed)
        self._chain.insert(0, grouped_seed)

    def check_results(self):
        links = wait(self._manager_links, timeout=-1)
        # read from available links.
        for link in links:
            """
            NOTE: This item is a copy of the item sent by the Manager modifications
            to the item will be reflected to its mirror item stored on the stem
            use the relevent data structure and ids item_id/chan_id to make necessary changes
            """
            item = link.recv()
            if isinstance(item, Seed):
                # Remove from waiting tasks
                del self._waiting_tasks[item._item_id]
                if isinstance(item._result, Bloom):
                    bloomed_item = item._result._item
                    hangle_bloom(item, bloomed_item)
                if item._chain_id:
                    self.unlink_from_chain(item)
            else:
                print("Invalid object sent through Pipe! ")
                raise TypeError

    def unlink_from_chain(self, item):
        # Remove from chains pending tasks
        self._pending_chain_tasks[item._chain_id].remove(item._item_id)
        # Check if no pending tasks for chain
        if not self._pending_chain_tasks[item._chain_id]:
            chain = self._pending_chains[item._chain_id]
            del self._pending_chains[item._chain_id]
            self._current_tasks[chain._item_id] = chain
        
    def handle_bloom(self, item, bloomed_item):
        if isinstance(bloomed_item, Seed):
            if item._chain_id:
                bloomed_item._chain_id = item._chain_id
                self._pending_chain_tasks[bloomed_item._chain_id].add(bloomed_item._item_id)
            self._current_tasks[bloomed_item._item_id] = bloomed_item

        elif isinstance(bloomed_item, Group):
            for group_item in bloomed_item._group:
                if item._chain_id:
                    group_item._chain_id = item._chain_id
                    self._pending_chain_tasks[group_item._chain_id].add(group_item._item_id)
                self._current_tasks[group_item._item_id] = group_item

        elif isinstance(bloomed_item, Chain):
            if item._chain_id:
                bloomed_item._chain_id = item._chain_id
                self._pending_chain_tasks[bloomed_item._chain_id].add(bloomed_item._item_id)
            self._current_tasks[bloomed_item._item_id] = bloomed_item

        else:
            raise TypeError
            
class Group():
    """
    A group is a collection of seeds and chains that can execute in parallel:
    """
    def __init__ (self, *args):
        self._group = []
        self._item_id = str(uuid.uuid1())
        self._chain_id = None
        for arg in iter(args):
            try:
                iargs = iter(arg)
                for iarg in iargs:
                    if isinstance(iarg, Chain):
                        self._group.append(iarg)
                    elif isinstance(iarg, Seed):
                        self._group.append(iarg)
                    else:
                        raise TypeError
            except:
                if isinstance(arg, Chain):
                    self._group.append(arg)
                elif isinstance(arg, Seed):
                    self._group.append(arg)
                else:
                    raise TypeError
        
    def run(self):
        chain = Chain(self)
        chain.run()

class Bloom():
    """
    A Bloom is a oject that is returned from a task that contains a Seed, Group, or Chain
    that replaces a seed  
    """
    def __init__(self, item):
        if isinstance(item, Seed) or isinstance(item, Group) or isinstance(item, Chain):
            self._item = item
        else:
            print("A Bloom object must contain a Seed Group or Chain", file=sys.stderr)
            raise TypeError
    
class Seed():
    """
    A Seed onject reflects the base object to be executed. 
    """
    def __init__(self, func, *args, **kwargs):
        self._func = func
        self._args = args
        self._kwargs = kwargs
        self._item_id = str(uuid.uuid1())
        self._chain_id = None
        self._manager = None
        self._result = None
        self._attr_list = {}

    def set_manager(self, manager):
        self._manager = manager
        return self

    def run(self):
        group = Group(self)
        group.run()
        
    def print(self):
        print(self._function, self._args, self._kwargs)
    
    def set_result(self, result):
        self._result = result

    def set(self, attr, *args, **kwargs):
        self._attr_list[attr] = {"args":args, "kwargs":kwargs}
        return self

def run_manager(name):
    
    p_read, c_write = Pipe()
    c_read, p_write = Pipe() 
    pid = os.fork()
    
    # Stem
    if pid:
        return p_read, p_write

    # Manager
    else:
        read = c_read
        write = c_write
        time.sleep(1) 
        tasks = {}

        m = vine.Manager(port=[9123,9143], name=name)
        while(True):
            while read.poll():
                try:
                    item = read.recv() 
                    if isinstance(item, Seed):
                        task = vine.PythonTask(item._func, *item._args, **item._kwargs)
                        try:
                            for attr in item._attr_list:
                                func = getattr(task, attr)
                                func(*item._attr_list[attr]["args"], **item._attr_list[attr]["kwargs"])
                        # TODO error handling
                        except:
                            pass
                        task.set_cores(1)
                        m.submit(task)
                        tasks[task.id] = item
                except:
                    raise RuntimeError
                    exit(1)

            while not m.empty():
                task = m.wait(5)
                if task:
                    # TODO set results and error handling
                    item = tasks[task.id]
                    item.set_result(task.output)
                    write.send(item)
                    del tasks[task.id]

