import sys
import os
import select
import time
import pickle
import cloudpickle
import uuid
import ndcctools.taskvine as vine
from multiprocessing import Pipe
from multiprocessing.connection import wait

class StemObject():
    def __init__(self):
        self._item_id = str(uuid.uuid1())
        self._chain_id = None
        self.i = True
        self.full_map = False

    
    def map(self, from_domain="all", to_range="all")
        if from_domain == "all" or to_range == "all":
            self._full_map = True
        else:
            self._domain = from_domain
            self._range = to_range

    @property
    def i(self):
        self.i = True
        return i

class Chain(StemObject):
    """
    A chain is group of tasks or groups that are executed to be executed sequentially.
    by default, running a singular group or seed eventually executes chain(group(seed))
    
    _chain - list of Seed and Group stem objects to execute sequentially
    _current_link - current StemObject (Seed or Group) currently being executed
    _current_items - mapping of current item_ids to item objects (Not yet sent, to a manager) 
    _waiting_items - mapping of waiting item_ids to item objects (sent to manager, waiting for result) 
    _pending_chains - mapping of pending chains waiting for sub items to be complete
    _pending_chain_items - mapping from chain_ids to the chains pending items in the queue
    _manager_links - list of read connection fds in which the Stem checks for messages
    _managers - mapping of manager names to manager connection objects
    _default_mgr - generated id of a default manager for a given chain, used if nno manager is specified during submission
    
    """
    def __init__ (self, *args):
        self._chain = []
        self._current_link = None
        self._current_items = {}
        self._waiting_items = {}
        self._pending_chains = {}
        self._pending_chain_items = {}
        self._item_mapping = {}
        self._manager_links = []
        self._managers = {}
        self._default_mgr = str(uuid.uuid1())
    
    
        # Add Stem Objects to the Chain. 
        # NOTE: the order of the objects determine execution order.
        # NOTE: Chains can not bee added to Chains. i.e. (Chain(Chain())) is invalid. However, Chain(Group(Chain)) is valid.
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
    
    # When deleting a master chain we send messages to kill all managers.
    def __del__(self):
        for manager in self._managers:
            self._managers[manager]["write"].send("kill")
    
    # Execute Stem objects within a chain in order
    def run(self):        
        # When a Chain is called wirh run() it becomes the master chain.
        # The master chain maintains mappings of results from the previous link that has been executed
        # Additionally, the current links results are kept. This is used when mapping outputs to inputs between links 
        self._master_item_mapping = {}
        self._master_previous_results = []
        self._master_current_results = []
        while self.pop_link:
            link = self._current_link
            # Execution of a Group of Stem objects
            if isinstance(link, Group):
                self.exec_group(link)
                       
            # Execution of a Single Seed object
            elif isinstance(link, Seed):
                self.exec_single_seed(link)

    # Set _currrent_link to the next available link. Returns False if there are no more links            
    def pop_link(self):
        if self._chain:
            self._current_link = self._chain.pop(0)
            return True
        else:
            return False
        
    # Execute a Group of Stem Objects.
    # NOTE: Each object can execute in Parallel
    # NOTE: When executing a SubChain, significant considerations need to be made.
    def exec_group(self, group):
        # Queue inital items and create mapping for value
        count = 0
        for item in group._group:
            self._current_tasks[item._item_id] = item
            self._item_mapping[item._item_id] = count
            count += 1 
        # Assign previous results to tasks
        if group._from_range and group._to_range:
            pass
        elif group._full_map:
            pass
        # Exceute current link as a group.
        while self._current_tasks or self._waiting_tasks: 
            # Queue current tasks
            for item in list(self._current_tasks.values()):
                if isinstance(item, Seed):
                    exec_sub_seed(item)
                elif isinstance(item, Chain):
                    exec_sub_chain(item)
                else:
                    del self._current_tasks[item._item_id]

            # check for results from managers.
            self.check_results() 
        self._previous_results = self._current_results
        self._current_results = []

    def exec_single_seed(self, seed):
        grouped_seed = Group(seed)
        grouped_seed.map(seed._domain, seed._range)
        self._chain.insert(0, grouped_seed)

    def exec_sub_chain(self, chain):
        if item._chain:
            chain_link = item._chain.pop(0)
        else:
            chain_link = None
            # set result to master chain
            if item._chain_id:
                # This is a subchain pass on the result 
                self.unlink_from_chain(chain)
        if isinstance(chain_link, Seed):
            # either load result from master results OR previous results if within a sub chain
            self._current_tasks[chain_link._item_id] = chain_link
            self._pending_chain_tasks[item._item_id] = {chain_link._item_id}
            self._pending_chains[item._item_id] = item
            chain_link._chain_id = item._item_id
            del self._current_tasks[item._item_id]

        elif isinstance(chain_link, Group):
            # load results from master results OR previous results
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

    def exec_sub_seed(self, seed):
        if seed._item_id in self._item_mapping and mapping:
            if full_map:
                item.update_args(self._previous_results)
            else:
                domain_index = self._item_mapping[item._item_id]//group._range
                start_index = domain_index*group._domain
                stop_index = domain_index*group._domain+group._domain
                item.update_args(self._previous_results[start_index:stop_index])

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
                    self.handle_bloom(item, item._result._item)
                if item._chain_id:
                    self.unlink_from_chain(item)
            else:
                print("Invalid object sent through Pipe!")
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
        # bloomed_item = cloudpickle.loads(bloomed_item)
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
            
class Group(StemObject):
    """
    A group is a collection of seeds and chains that can execute in parallel:
    """
    def __init__ (self, *args):
        self._group = []
        super().__init__
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

    
class Seed(StemObject):
    """
    A Seed onject reflects the base object to be executed. 
    """
    def __init__(self, func, *args, **kwargs):
        #self._func = func
        #self._args = args
        #self._kwargs = kwargs
        # TODO: This keeps The connection object from complaining when sending certain object via the Commuincation Pipe
        # However, this may cause some overhead so a better solution may need to be explored
        self._srl = cloudpickle.dumps((func, args, kwargs))
        self._item_id = str(uuid.uuid1())
        self._manager = None
        self._result = None
        self._attr_list = {}
        super().__init__()

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


class Bloom():
    """
    A Bloom is a oject that is returned from a task that contains a Seed, Group, or Chain
    that replaces a seed  
    """
    def __init__(self, item):
        if isinstance(item, Seed) or isinstance(item, Group) or isinstance(item, Chain):
            self._item = item
        else:
            print("Error: A Bloom object must contain a Seed Group or Chain!", file=sys.stderr)
            raise TypeError

def run_manager(name):
    
    p_read, c_write = Pipe()
    c_read, p_write = Pipe() 
    pid = os.fork()
    
    # Stem
    if pid:
        return p_read, p_write
    

    # Manager
    else:
        def exec_func(srl, options):
            func, args, kwargs = cloudpickle.loads(srl)
            return func(*args, **kwargs)

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
                        task = vine.PythonTask(exec_func, item._srl, None)
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
                    elif isinstance(item, str) and item == "kill":
                        exit(1)
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

