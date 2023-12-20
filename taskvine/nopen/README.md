# nopen

nopen hijacks task system calls and chooses to allow or deny the call based on a set of rules. These rules are defined in a rules.txt file located in the working directory of the manager script. This file contains paths along with the associated permissions. In the manger script nopen is activated through a method on the task. When the task is run, nopen copies the rules file and the nopen library into the workers directory and sets LD_PRELOAD for the task. This captures the system calls open, stat, and unlink and supports enforcing permissions for reading, writing, creating and deleting files, and stat. When a violation occurs nopen either has the program exit or shows returns a no entry for the file requested.

Nopen currently hijacks the system calls for open, stat, and unlink.

## Rules file

An example rules file is show here:

```
/dev/ RWS
/usr/lib64/ RS
/etc/ RS
. RWSDN
/ S
#/tmp/
```

* A file will follow the permissions of the first rule that matches its path
* `.`  applies the permissions the the current directory of the program
* `/` applies permissions to every file
* Starting a line with `# `  allows rules to be ignored

### Key

| Letter | Definition         |
| ------ | ------------------ |
| R      | open : read        |
| W      | open : write       |
| N      | open : create file |
| S      | stat               |
| D      | unlink : delete    |

## Environment Variables

### `NOPEN_RULES`

Allows to specify an alternate rules file

### `NOPEN_HANDLE`

Sets what to do when a nopen violation occurs

* Defaults to exit

`NOPEN_HANDLE_STAT` can also be set to set permissions specifically for stat

* Defaults to log

#### Options

| option | definition                                                   |
| ------ | ------------------------------------------------------------ |
| exit   | nopen forces program to exit with exit code 1 on violation   |
| enoent | nopen returns -1 and sets the errno to ENOENT                |
| log    | nopen prints the violation to stderr but lets the program proceed |



## Errata

* nopen does not compile with cctools and make must be run in this directory
* lib-nopen.so must be in the current directory of the manager script 
