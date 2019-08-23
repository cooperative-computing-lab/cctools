# Cooperative Computing Tools Documentation

**[Getting Started](install.html)**

## Components

- [**Makeflow**](makeflow) is a workflow system for parallel and distributed
  computing that uses a language very similar to Make. Using Makeflow, you can
  write simple scripts that easily execute on hundreds or thousands of
  machines. 

- [**JX Workflow Language**](jx) is the "advanced" language used by the
  Makeflow workflow engine. JX is an extension of standard JSON expressions, so
  if you are familiar with those from another language, you will find it easy
      to get started. 

- [**Work Queue**](work_queue) is a system and library for creating and
  managing scalable master-worker style programs that scale up to thousands of
  machines on clusters, clouds, and grids. Work Queue programs are easy to
  write in [Python](python api), [Perl](perl api) or [C](C api).

- [**Resource Monitor**](resource_monitor) is a tool to monitors the cpu,
  memory, io, and disk usage of applications running in distributed systems,
  and can optionally enforce limits on each resource. The monitor can be
  compiled to a single executable that is easily deployed to track executable
  file, or it can be used as a library to track the execution of [Python
  functions](python api).

- [**Parrot**](parrot) is a transparent user-level virtual filesystem that
  allows any ordinary program to be attached to many different remote storage
  systems, including HDFS, iRODS, Chirp, and FTP. 


- [**Chirp**](chirp)  is a personal user-level distributed filesystem that
  allows unprivileged users to share space securely, efficiently, and
  conveniently. When combined with Parrot, Chirp allows users to create custom
  wide-area distributed filesystems. 

- [**Catalog Server**](catalog)


## Technical Previews and Example Applications

- [**AWE**](awe)

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




## Further Information

[**Networking Configuration**](network)

[**Man pages**](man_pages.md)

