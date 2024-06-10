# Shepherd - Service Orchestration and Monitoring Tool

Shepherd is a service orchestration and monitoring tool system that launches and monitors applications according
to a specified configuration. It manages dependencies and synchronizes the startup of applications based on 
their interdependencies defined in a YAML configuration file. Shepherd is designed to handle complex dependency
relations among multiple processes, ensuring that each application starts only after its dependencies have reached
a specified state. 



## Key Features

- **Dependency Management:** Shepherd initiates applications based on the internal state of others.
It supports `any` and `all` dependency modes, allowing for flexible dependency configurations.

[//]: # (- **User Defined States:** Todo) 

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

## Getting Started with Shepherd: A Hello World Example
Shepherd simplifies complex application workflows. Here’s a simple example to demonstrate how to use Shepherd for 
scheduling dependent programs.

##### 1. Create Sample Scripts

Create two shell scripts named `program1.sh` and `program2.sh` with the following content:

```shell
#!/bin/bash

echo "Starting program..."
sleep 5
echo "Program completed"
```

##### 2. Create a Shepherd Configuration File

Create a Shepherd configuration file to schedule `program2` to start only after` program1` has successfully completed its 
execution. Save the following content as` program-config.yml`:
```yaml
services:
  program1:
    command: "./program1.sh"
  program2:
    command: "./program2.sh"
    dependency:
      items:
        program1: "action_success"  # Start program2 only after program1 succeeds
output:
  state_times: "state_transition_times.json"
max_run_time: 60  # Optional: Limit total runtime to 60 seconds
```

##### 3. Run Shepherd

To run the configuration with Shepherd, use the following command:
```shell
<shepherd_executable> program-config.yml
```
If you are running the python source, then run

```shell
python3 shepherd.py program-config.yml
```

#### Understanding the workflow
With this simple configuration, Shepherd will:
1. Execute `program1.sh`.
2. Monitor the internal states of the program.
3. Start `program2.sh` only after `program1.sh `succeeds.
4. Create state_transition_times.json, which will look similar to this:

```json
{
  "program1": {
    "initialized": 0.246384859085083,
    "started": 0.24660515785217285,
    "success": 5.349443197250366,
    "final": 5.350545883178711
  },
  "program2": {
    "initialized": 0.2456510066986084,
    "started": 5.351618051528931,
    "success": 10.464960098266602,
    "final": 10.465446949005127
  }
}
```

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

