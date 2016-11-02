import hashlib
import os
import sys


paths_to_jobs = {}

class SimpleDag(object):
  def __init__(self):
    self.name = "dag"
    self.root_nodes = []

class SimpleDagJob(object):
  def __init__(self, path, local_path = ""):
    self.cached_path = path
    self.local_path = local_path
    paths_to_jobs[self.cached_path] = self
    self.command = ""
    self.batch_job_info = {}
    self.input_files= []
    self.output_files= []
    self.ancestors = []
    self.descendants = []
    self.recover_run_info()
    self.create_input_output_files()
    self.recover_ancestors()
    self.recover_descendants()

  def add_input_file(self, file_path):
    self.input_files.append(SimpleDagFile(file_path))

  def add_output_file(self, file_path):
    self.output_files.append(SimpleDagFile(file_path, self.command))


  def create_input_output_files(self):
    input_file_directories = os.path.join(self.cached_path, "input_files")
    input_file_paths = [os.path.realpath(os.path.join(input_file_directories, f_path)) for f_path in os.listdir(input_file_directories)]

    output_file_dir = os.path.join(self.cached_path, "outputs")
    output_file_paths = [os.path.join(output_file_dir, f_path) for f_path in os.listdir(output_file_dir)]

    for o_path in output_file_paths:
        self.add_output_file(o_path)

    for i_path in input_file_paths:
      self.add_input_file(i_path)

  def recover_ancestors(self):
    path = os.path.join(self.cached_path, "ancestors")
    try:
      with open(path, "r") as f:
        line = f.readline().rstrip()
        while line:
          cache_prefix = line[0:2]
          ancestor_path = os.path.realpath(os.path.join(self.cached_path, "../../", cache_prefix, line))
          if ancestor_path in paths_to_jobs:
            self.ancestors.append(paths_to_jobs[ancestor_path])
          else:
            new_dag_job = SimpleDagJob(ancestor_path)
            self.ancestors.append(new_dag_job)
            paths_to_jobs[ancestor_path] = new_dag_job
          line = f.readline().rstrip()
    except IOError:
      pass

  def recover_descendants(self):
    descendant_dir = os.path.join(self.cached_path, "descendants")
    descendant_job_paths = [os.path.realpath(os.path.join(self.cached_path,"descendants", path)) for path in os.listdir(descendant_dir)]
    for descendant_path in descendant_job_paths:
      if descendant_path in paths_to_jobs:
        self.descendants.append(paths_to_jobs[descendant_path])
      else:
        new_dag_job = SimpleDagJob(descendant_path)
        self.descendants.append(new_dag_job)
        paths_to_jobs[descendant_path] = new_dag_job

  def recover_run_info(self):
    run_info_path = os.path.join(self.cached_path, "run_info")
    with open(run_info_path, "r") as f:
      self.command = f.readline().rstrip()
      self.batch_job_info['submitted'] = int(f.readline().rstrip())
      self.batch_job_info['started'] = int(f.readline().rstrip())
      self.batch_job_info['finished'] = int(f.readline().rstrip())
      self.batch_job_info['exited_normally'] = int(f.readline().rstrip())
      self.batch_job_info['exit_code'] = int(f.readline().rstrip())
      self.batch_job_info['exit_signal'] = int(f.readline().rstrip())



  def print_job(self):
    print "file: ", self.local_path
    print "Created by job cached at path = ", self.cached_path
    print "Command used to create this file = ", self.command
    print "input files for this job: "
    for i_file in self.input_files:
      print "\tfile: ", i_file.file_name
    print "makeflow file cached at path = ", get_makeflow_path(self)
    print ""

class SimpleDagFile(object):
  def __init__(self, file_path, command = ""):
    self.command = ""
    self.file_path = file_path
    self.file_name = file_path.split(',')[-1]


def recreate_job(job_path, local_):

  new_dag_job = SimpleDagJob(job_path, local_)
  node = new_dag_job
  return node

def get_makeflow_path(node):
  while len(node.ancestors) != 0:
    node = node.ancestors[0]
  return os.path.join(node.cached_path, "source_makeflow")

def get_dag_roots(dag_node):
  nodes_seen = {}
  root_nodes = []
  search(dag_node, nodes_seen, root_nodes)
  return root_nodes[0].path


def search(dag_node, nodes_seen, root_nodes):
  nodes_seen[dag_node.path] = 1
  if len(dag_node.ancestors) == 0:
    root_nodes.append(dag_node)
  for node in dag_node.ancestors:
    if node.path not in nodes_seen:
      search(node, nodes_seen, root_nodes)
  for node in dag_node.descendants:
    if node.path not in nodes_seen:
      search(node, nodes_seen, root_nodes)


if __name__ == "__main__":
  arguments = sys.argv[1:]
  if len(arguments) != 1:
    raise Exception("suitable arguments not found")

  file_name = arguments[0]
  sha1_hash = hashlib.sha1()
  with open(file_name, "r") as f:
    line = f.readline()
    while line:
      sha1_hash.update(line)
      line = f.readline()
  hex_digest = sha1_hash.hexdigest()

  file_path = "/tmp/makeflow.cache.{}/files/{}/{}".format(os.geteuid(), hex_digest[0:4], hex_digest)
  if os.path.islink(file_path):
    print "Recovering file {}".format(file_name)
    # new_dag = SimpleDag()
    resolved_job_path = os.path.realpath(file_path)
    node = recreate_job(resolved_job_path, file_name)
    node.print_job()
  else:
    print "File has not been cached"

