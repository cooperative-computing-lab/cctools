# TaskVine Log File Formats

## Performance Log Format

The performance log is a sequence of records, recorded at
each significant change in an integer metric such as the number of tasks submitted,
running, and so forth.
The first row always contains the name of the columns, which correspond to
values that can be obtained from `vine_stats`.  The first column is a Unix timestamp
with microsecond resolution.

Here is an example of the first few rows and columns:

```text
# timestamp workers_connected workers_init workers_idle workers_busy workers_...
1602165237833411 0 0 0 0 0 0 0 0 0 0 0 0 5 0 0 0 5 0 0 0 0 0 1602165237827668 ...
1602165335687547 1 0 0 1 1 1 0 0 0 0 0 0 4 1 0 0 5 0 0 0 0 0 1602165237827668 ...
1602165335689677 1 0 0 1 1 1 0 0 0 0 0 0 4 1 1 1 5 1 0 0 0 0 1602165237827668 ...
...
```

## Taskgraph Log Format

The `taskgraph` log contains a sequence of records describing the basic
properties of each file and task, and the relationships between the two.
It is suitable for determining the provenance of files and tasks, and
can be used to display the visual structure of a workflow.

Each line is ASCII text with words delimited by spaces, except where quoted.
Lines beginning with `#` are comments.  A file record consists of the word
`FILE` followed by the unique file id, the quoted source path of the file,
and the size of the file, if known.
A task record consists of the word `TASK` followed by the unique task ID,
the quoted name of the task, the keyword `INPUTS` followed by a list of input file IDs,
and the keyword `OUTPUTS` followed by a list of output file IDs.

```text
# taskvine taskgraph version 2
# TASK taskid "program" INPUTS fileid1 fileid fileid3 ... OUTPUTS fileid4 fileid5 ...
# FILE fileid "source" size
FILE file-meta-9c24a99af02ce7f488fcc7461fba2423 "convert.sfx" 350768
FILE url-rnd-qjmltcbhuctjrfl "A-Cat.jpg" 163047
FILE temp-rnd-sfscsbmjstfkvob "temp" 0
TASK T1 "convert.sfx" INPUTS file-meta-9c24a99af02ce7f488fcc7461fba2423 url-rnd-qjmltcbhuctjrfl OUTPUTS temp-rnd-sfscsbmjstfkvob 
TASK T4 "convert.sfx" INPUTS file-meta-9c24a99af02ce7f488fcc7461fba2423 url-rnd-qjmltcbhuctjrfl OUTPUTS temp-rnd-wqtpljproftkoym 
TASK T23 "convert.sfx" INPUTS file-meta-9c24a99af02ce7f488fcc7461fba2423 url-rnd-qjmltcbhuctjrfl OUTPUTS temp-rnd-ycbskigfhodnnuj 
TASK T5 "convert.sfx" INPUTS file-meta-9c24a99af02ce7f488fcc7461fba2423 url-rnd-qjmltcbhuctjrfl OUTPUTS temp-rnd-evtwiteajlpgwjn 
...
```

## Transactions Log Format

The first few lines of the log document the possible log records:

```text
# time manager_pid MANAGER manager_pid START|END time_from_origin
# time manager_pid WORKER worker_id CONNECTION host:port
# time manager_pid WORKER worker_id DISCONNECTION (UNKNOWN|IDLE_OUT|FAST_ABORT|FAILURE|STATUS_WORKER|EXPLICIT)
# time manager_pid WORKER worker_id RESOURCES {resources}
# time manager_pid WORKER worker_id CACHE_UPDATE filename size_in_mb wall_time_us start_time_us
# time manager_pid WORKER worker_id TRANSFER (INPUT|OUTPUT) filename size_in_mb wall_time_us start_time_us
# time manager_pid CATEGORY name MAX {resources_max_per_task}
# time manager_pid CATEGORY name MIN {resources_min_per_task_per_worker}
# time manager_pid CATEGORY name FIRST (FIXED|MAX|MIN_WASTE|MAX_THROUGHPUT) {resources_requested}
# time manager_pid TASK task_id WAITING category_name (FIRST_RESOURCES|MAX_RESOURCES) attempt_number {resources_requested}
# time manager_pid TASK task_id RUNNING worker_id (FIRST_RESOURCES|MAX_RESOURCES) {resources_allocated}
# time manager_pid TASK task_id WAITING_RETRIEVAL worker_id
# time manager_pid TASK task_id RETRIEVED (SUCCESS|UNKNOWN|INPUT_MISSING|OUTPUT_MISSING|STDOUT_MISSING|SIGNAL|RESOURCE_EXHAUSTION|MAX_RETRIES|MAX_END_TIME|MAX_WALL_TIME|FORSAKEN) {limits_exceeded} {resources_measured}
# time manager_pid TASK task_id DONE (SUCCESS|UNKNOWN|INPUT_MISSING|OUTPUT_MISSING|STDOUT_MISSING|SIGNAL|RESOURCE_EXHAUSTION|MAX_RETRIES|MAX_END_TIME|MAX_WALL_TIME|FORSAKEN) exit_code
# time manager_pid LIBRARY library_id (WAITING|SENT|STARTED|FAILURE) worker_id

```

Lowercase words indicate values, and uppercase indicate constants. A bar (|) inside parentheses indicate a choice of possible constants. Variables encased in braces {} indicate a JSON dictionary. Here is an example of the first few records of a transactions log:

```
1679929304405580 4107108 MANAGER 4107108 START 0
1679929315785718 4107108 TASK 1 WAITING default FIRST_RESOURCES 1 {"cores":[1,"cores"]}
1679929315789781 4107108 TASK 2 WAITING default FIRST_RESOURCES 1 {"cores":[1,"cores"]}
1679929315791349 4107108 TASK 3 WAITING default FIRST_RESOURCES 1 {"cores":[1,"cores"]}
1679929315792852 4107108 TASK 4 WAITING default FIRST_RESOURCES 1 {"cores":[1,"cores"]}
1679929315794343 4107108 TASK 5 WAITING default FIRST_RESOURCES 1 {"cores":[1,"cores"]}
...
```

With the transactions log, it is easy to track the lifetime of a task. For example, to print the lifetime of the task with id 1, we can simply do:

```
$ grep 'TASK \<1\>' my.tr.log
1599244364466668 16444 TASK 1 WAITING default FIRST_RESOURCES {"cores":[1,"cores"],"memory":[800,"MB"],"disk":[500,"MB"]}
1599244400311044 16444 TASK 1 RUNNING 10.32.79.143:48268  FIRST_RESOURCES {"cores":[4,"cores"],"memory":[4100,"MB"],...}
1599244539953798 16444 TASK 1 WAITING_RETRIEVAL 10.32.79.143:48268
1599244540075173 16444 TASK 1 RETRIEVED SUCCESS  0  {} {"cores":[1,"cores"],"wall_time":[123.137485,"s"],...}
1599244540083820 16444 TASK 1 DONE SUCCESS  0  {} {"cores":[1,"cores"],"wall_time":[123.137485,"s"],...}
```

The statistics available are:

| Field | Description |
|-------|-------------|
|       | **Stats for the current state of workers** |
| workers_connected	    | Number of workers currently connected to the manager |
| workers_init          | Number of workers connected, but that have not send their available resources report yet |
| workers_idle          | Number of workers that are not running a task |
| workers_busy          | Number of workers that are running at least one task |
| workers_able          | Number of workers on which the largest task can run |
|||
|       | **Cumulative stats for workers** |
| workers_joined        | Total number of worker connections that were established to the manager |
| workers_removed       | Total number of worker connections that were released by the manager, idled-out, slow, or lost |
| workers_released      | Total number of worker connections that were asked by the manager to disconnect |
| workers_idled_out     | Total number of worker that disconnected for being idle |
| workers_slow          | Total number of worker connections terminated for being too slow |
| workers_blacklisted   | Total number of workers blacklisted by the manager (includes workers_slow) |
| workers_lost          | Total number of worker connections that were unexpectedly lost (does not include idled-out or slow) |
|||
|       | **Stats for the current state of tasks** |
| tasks_waiting         | Number of tasks waiting to be dispatched |
| tasks_on_workers      | Number of tasks currently dispatched to some worker |
| tasks_running         | Number of tasks currently executing at some worker |
| tasks_with_results    | Number of tasks with retrieved results and waiting to be returned to user |
|||
|       | **Cumulative stats for tasks** |
| tasks_submitted            | Total number of tasks submitted to the manager |
| tasks_dispatched           | Total number of tasks dispatch to workers |
| tasks_done                 | Total number of tasks completed and returned to user (includes tasks_failed) |
| tasks_failed               | Total number of tasks completed and returned to user with result other than VINE_RESULT_SUCCESS |
| tasks_cancelled            | Total number of tasks cancelled |
| tasks_exhausted_attempts   | Total number of task executions that failed given resource exhaustion |
|||
|       | **Manager time statistics (in microseconds)** |
| time_when_started  | Absolute time at which the manager started |
| time_send          | Total time spent in sending tasks to workers (tasks descriptions, and input files) |
| time_receive       | Total time spent in receiving results from workers (output files) |
| time_send_good     | Total time spent in sending data to workers for tasks with result VINE_RESULT_SUCCESS |
| time_receive_good  | Total time spent in sending data to workers for tasks with result VINE_RESULT_SUCCESS |
| time_status_msgs   | Total time spent sending and receiving status messages to and from workers, including workers' standard output, new workers connections, resources updates, etc. |
| time_internal      | Total time the manager spents in internal processing |
| time_polling       | Total time blocking waiting for worker communications (i.e., manager idle waiting for a worker message) |
| time_application   | Total time spent outside vine_wait |
|||
|       | **Wrokers time statistics (in microseconds)** |
| time_workers_execute             | Total time workers spent executing done tasks |
| time_workers_execute_good        | Total time workers spent executing done tasks with result VINE_RESULT_SUCCESS |
| time_workers_execute_exhaustion  | Total time workers spent executing tasks that exhausted resources |
|||
|       | **Transfer statistics** |
| bytes_sent      | Total number of file bytes (not including protocol control msg bytes) sent out to the workers by the manager |
| bytes_received  | Total number of file bytes (not including protocol control msg bytes) received from the workers by the manager |
| bandwidth       | Average network bandwidth in MB/S observed by the manager when transferring to workers |
|||
|       | **Resources statistics** |
| capacity_tasks      | The estimated number of tasks that this manager can effectively support |
| capacity_cores      | The estimated number of workers' cores that this manager can effectively support |
| capacity_memory     | The estimated number of workers' MB of RAM that this manager can effectively support |
| capacity_disk       | The estimated number of workers' MB of disk that this manager can effectively support |
| capacity_instantaneous       | The estimated number of tasks that this manager can support considering only the most recently completed task |
| capacity_weighted   | The estimated number of tasks that this manager can support placing greater weight on the most recently completed task |
|||
| total_cores       | Total number of cores aggregated across the connected workers |
| total_memory      | Total memory in MB aggregated across the connected workers |
| total_disk	    | Total disk space in MB aggregated across the connected workers |
|||
| committed_cores   | Committed number of cores aggregated across the connected workers |
| committed_memory  | Committed memory in MB aggregated across the connected workers |
| committed_disk    | Committed disk space in MB aggregated across the connected workers |
|||
| max_cores         | The highest number of cores observed among the connected workers |
| max_memory        | The largest memory size in MB observed among the connected workers |
| max_disk          | The largest disk space in MB observed among the connected workers |
|||
| min_cores         | The lowest number of cores observed among the connected workers |
| min_memory        | The smallest memory size in MB observed among the connected workers |
| min_disk          | The smallest disk space in MB observed among the connected workers |
|||
| manager_load       | In the range of [0,1]. If close to 1, then the manager is at full load <br /> and spends most of its time sending and receiving taks, and thus <br /> cannot accept connections from new workers. If close to 0, the <br /> manager is spending most of its time waiting for something to happen. |
