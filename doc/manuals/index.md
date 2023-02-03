![](logos/cctools-logo.png)

# Cooperative Computing Tools Documentation

## Getting Started

- **[About](about.md)**
- **[Installation](install)**
- **[Getting Help](help)**

## Software Components

- [**Makeflow**](makeflow) is a workflow system for parallel and distributed
  computing using either the classic Make syntax or the more advanced
  [JX Workflow Language](jx-workflow).   Using Makeflow, you can
  write simple scripts that easily execute on hundreds or thousands of
  machines. 

- [**Work Queue**](work_queue) is a system and library for creating and
  managing scalable manager-worker style programs that scale up to thousands of
  machines on clusters, clouds, and grids. Work Queue programs are easy to
Python ([example](work_queue/examples/work_queue_example.py)|[api](api/html/namespaceWorkQueuePython.html))
Perl   ([example](work_queue/examples/work_queue_example.pl)|[api](http://ccl.cse.nd.edu/software/manuals/api/html/work__queue_8h.html)),
or C   ([example](work_queue/examples/work_queue_example.c)|[api](api/html/work__queue_8h.html))
.

- [**Resource Monitor**](resource_monitor) is a tool to monitors the cpu,
  memory, io, and disk usage of applications running in distributed systems,
  and can optionally enforce limits on each resource. The monitor can be
  compiled to a single executable that is easily deployed to track executable
  file, or it can be used as a library to track the execution of [Python
  functions](api/html/namespaceresource__monitor.html).

- [**Parrot**](parrot) is a transparent user-level virtual filesystem that
  allows any ordinary program to be attached to many different remote storage
  systems, including HDFS, iRODS, Chirp, and FTP. 

- [**Chirp**](chirp)  is a personal user-level distributed filesystem that
  allows unprivileged users to share space securely, efficiently, and
  conveniently. When combined with Parrot, Chirp allows users to create custom
  wide-area distributed filesystems. 

- [**Catalog Server**](catalog) is a common facility used to monitor
  running services, workflows, and tasks.  It provides real-time status
  and historical data on all components of the CCTools.

## Research Prototypes

- [**TaskVine**](taskvine) is our third-generation workflow system
for building data-intensive workflow applications.  TaskVine applications
consist of many chained tasks that pull in external data into a cluster,
where computed results can be cached and re-used by later tasks,
even in successive workflows.

- [**Accelerated Weighted Ensemble**](awe) (AWE) is an ensemble
  molecular dynamics applications that uses Work Queue to scale
  out molecular simulations to thousands of GPUs on multipel clusters.

- [**Confuga**](confuga) is an active storage cluster file system designed for
  executing DAG-structured scientific workflows. It is used as a collaborative
  distributed file system and as a platform for execution of scientific
  workflows with full data locality for all job dependencies.

- [**Umbrella**](umbrella) is a tool for specifying and materializing execution
  environments, from the hardware all the way up to software and data. Umbrella
  parses a task specification and determines the minimum mechanism necessary to
  run it. It downloads missing dependencies, and executes the application
  through the available minimal mechanism, which may be direct execution, a
  system container, a virtual machine, or submissions to a cloud and cluster environments.

- [**Prune**](prune) Prune is a system for executing and precisely preserving
  scientific workflows to ensure reproducibility.  Every task to be executed in
  a workflow is wrapped in a functional interface and coupled with a strictly
  defined environment.

## Reference Information

- [**Man Pages**](man_pages.md)

- [**JX Expression Language**](jx)

- [**JX Workflow Language**](jx-workflow)

- [**Chirp Protocol Specification**](chirp/chirp_protocol.md)

- [**Networking Configuration**](network)

- [**API**](api/html/index.html)
