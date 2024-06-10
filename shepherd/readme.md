# Shepherd - Service Orchestration and Monitoring Tool

Shepherd is a service orchestration and monitoring tool system that launches and monitors applications according
to a specified configuration. It manages dependencies and synchronizes the startup of applications based on 
their interdependencies defined in a YAML configuration file. Shepherd is designed to handle complex dependency
relations among multiple processes, ensuring that each application starts only after its dependencies have reached
a specified state. 



## Key Features

- **Dependency Management:** Shepherd initiates applications based on the internal state of others.
It supports `any` and `all` dependency modes, allowing for flexible dependency configurations.

- **State Monitoring:** Shepherd continuously monitors the internal state of each application by 
analyzing standard output and the contents of any specified files. It updates the state in real-time based 
on specific keywords found in these files.

- **Service as Task:** Shepherd treats long-running services as tasks with defined initial and final states, 
facilitating structured and predictable service management.

[//]: # (# Todo)

[//]: # (- Makes a workflow of services a task. Complicated workflow -> one task.)

[//]: # (- Add drone workflow)

- **Graceful Shutdown:** Shepherd provides three modes of shutdown: a predetermined maximum runtime, a stop signal 
file, and response to external interrupts, ensuring a controlled and safe cessation of services.

- **Success and Failure Reporting:** Shepherd allows for detailed definitions of success and failure conditions for 
each application and the overall workflow. It also provides configurable actions to be taken in specific failure 
scenarios.

## Sample Configuration
Shepherd requires a YAML configuration file to specify details about the services it needs to manage.
Below is an explanation of the configuration parameters and an example:

```yaml
services:
  program1:
    type: "action"
    command: "./program1.sh"
    stdout_path: "/tmp/log/program1_out.log"
    stderr_path: "/tmp/log/program1_error.log"
    state:
      log:
        ready: "program is ready"
        complete: "program is completed"
      file:
        path: "/tmp/program_1_out_2.log"
        states:
          waiting: "program is waiting for program2"
  program2:
    type: "service"
    command: "./program2.sh"
    stdout_path: "/tmp/log/program2_out.log"
    stderr_path: "/tmp/log/program2_error.log"
    state:
      log:
        ready: "program is ready"
        complete: "program is completed"
    dependency:
      mode: "all"
      items:
        program1: "ready"
  program3:
    type: "service"
    command: "./program3.sh"
    state:
      log:
        ready: "program is ready"
        complete: "program is completed"

output:
  state_times: "state_times.json"
  stdout_dir: "/tmp"

stop_signal: "/tmp/bashapp/stop"
max_run_time: 120

```
## Program State Transition Overview

The Shepherd tool manages program execution through a series of defined states, ensuring dependencies are met and final 
states are recorded. Every program has default states (`Initialized`, `Started`, and `Final`) and can have optional 
user-defined states. Programs transition from `Initialized` to `Started` once dependencies are satisfied, then move through 
user-defined states. Actions return `Action Success` on a zero return code and `Action Failure` otherwise, while services 
transition to `Service Failure` if they stop unexpectedly. Any program receiving a stop signal is marked as `Stopped`, and 
all programs ultimately transition to a `Final` state, reflecting their execution outcome.

![Test](diagram/dot/states.svg)

## Visualization

