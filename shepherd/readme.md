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

## Program State Transition Overview

The Shepherd tool manages program execution through a series of defined states, ensuring dependencies are met and final 
states are recorded. Every program has default states (`Initialized`, `Started`, and `Final`) and can have optional 
user-defined states. Programs transition from `Initialized` to `Started` once dependencies are satisfied, then move through 
user-defined states. Actions return `Action Success` on a zero return code and `Action Failure` otherwise, while services 
transition to `Service Failure` if they stop unexpectedly. Any program receiving a stop signal is marked as `Stopped`, and 
all programs ultimately transition to a `Final` state, reflecting their execution outcome.

![Test](diagram/dot/states.svg)


## Getting Started with Shepherd: A Hello World Example
Shepherd simplifies complex application workflows. Here’s a simple example to demonstrate how to use Shepherd for 
scheduling dependent programs.

#### 1. Create Sample Scripts

Create two shell scripts named `program1.sh` and `program2.sh` with the following content:

```shell
#!/bin/bash

echo "Starting program..."
sleep 5
echo "Program completed"
```

#### 2. Create a Shepherd Configuration File

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

#### 3. Run Shepherd

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

## Monitoring User-Defined States in Shepherd
Shepherd has the ability to monitor standard output (stdout) or any other file to detect 
user-defined states. These states can then be used as dependencies for other programs. 
This feature allows you to define complex workflows based on custom application states.

#### Example Scenario: Dynamic Dependencies
Here’s an example shell script that simulates a program with different states. Save this script as `program1.sh`, 
`program2.sh`, and `program3.sh`:
```bash
#!/bin/bash
START_TIME=$(date +%s)
startup_delay=5

while true; do
  echo "$(date +%s) - program is booting up"
  sleep 0.5

  if [[ $(date +%s) -gt $((START_TIME + startup_delay)) ]]; then
    echo "$(date +%s) - program is ready"
    break
  fi
done

READY_TIME=$(date +%s)
run_duration=30

while true; do
  echo "$(date +%s) - program is running"
  sleep 0.5

  if [[ $(date +%s) -gt $((READY_TIME + run_duration)) ]]; then
    echo "$(date +%s) - program is completed"
    break
  fi
done

```
This script simulates a program that boots up, becomes ready, runs for a while, and then completes. Shepherd can use 
the log output to determine when the program is ready for the next step in your workflow.

#### Shepherd Configuration with user-defined states
Below is a sample Shepherd configuration that uses custom states defined in the application's stdout. The configuration 
specifies that program2 will start only after program1 is "ready" and program3 is "complete".

Save the following content as `program-config.yml`:

```yaml
services:
  program1:
    command: "./program1.sh"
    state:
      log:
        ready: "program is ready"
        complete: "program is completed"
  program2:
    command: "./program2.sh"
    state:
      log:
        ready: "program is ready"
        complete: "program is completed"
    dependency:
      mode: "all"
      items:
        program1: "ready"
        program3: "complete"
  program3:
    command: "./program3.sh"
    state:
      log:
        ready: "program is ready"
        complete: "program is completed"
output:
  state_times: "state_transition_times.json"
max_run_time: 120
```

#### How This Configuration Works
- program1: Starts immediately. Shepherd monitors its output for "program is ready" and "program is completed".
- program3: Also starts immediately. Shepherd monitors it similarly to program1.
- program2: Starts only after BOTH of these conditions are met:
  - program1 reaches the "ready" state.
  - program3 reaches the "complete" state

- And this will also create the following state transition data:
```json
{
  "program1": {
    "initialized": 0.2520601749420166,
    "started": 0.2529749870300293,
    "ready": 5.447847843170166,
    "complete": 36.73489499092102,
    "success": 36.781131982803345,
    "final": 36.781386852264404
  },
  "program3": {
    "initialized": 0.252730131149292,
    "started": 0.25317811965942383,
    "ready": 5.451045989990234,
    "complete": 36.72722291946411,
    "success": 36.80730319023132,
    "final": 36.80773401260376
  },
  "program2": {
    "initialized": 0.25133585929870605,
    "started": 36.72884702682495,
    "ready": 42.47040295600891,
    "complete": 73.59736275672913,
    "success": 73.61198306083679,
    "final": 73.612459897995
  }
}
```
