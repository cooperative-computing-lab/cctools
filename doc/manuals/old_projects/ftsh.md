# Fault-Tolerant Shell (ftsh) Technical Manual

**Last edited: 27 August 2004**

The Fault Tolerant Shell (ftsh) is Copyright (c) 2003-2004 Douglas Thain and
Copyright (c) 2005 The University of Notre Dame. All rights reserved. This
software is distributed under the GNU General Public License. Please see the
file COPYING for details.

**Please use the following citation for FTSH:**

  * Douglas Thain and Miron Livny, [The Ethernet Approach to Grid Computing](http://www.cse.nd.edu/~dthain/papers/ethernet-hpdc12.pdf), IEEE High Performance Distributed Computing, August 2003.

## Introduction⇗

Shell scripts are a vital tool for integrating software. They are
indispensable for rapid prototyping and system assembly. Yet, they are
extraordinarily sensitive to errors. A missing file, a broken network, or a
sick file server can cause a script to barrel through its actions with
surprising results. It is possible to write error-safe scripts, but only with
extraordinary discipline and complexity.

The Fault Tolerant Shell (ftsh) aims to solve this problem by combining the
ease of scripting with precise error semantics. Ftsh is a balance between the
flexibility and power of script languages and the precision of most compiled
languages. Ftsh parses complete programs in order to eliminate run-time
errors. An exception-like structure allows scripts to be both succinct and
safe. A focus on timed repetition simplifies the most common form of recovery
in a distributed system. A carefully-vetted set of language features limits
the "surprises" that haunt system programmers.

As an example, consider this innocuous script written in Bourne Shell:
`#!/bin/sh cd /work/foo rm -rf bar cp -r /fresh/data . ` Suppose that the
`/work` filesystem is temporarily unavailable, perhaps due to an NFS failure.
The `cd` command will fail and print a message on the console. The shell will
ignore this error result -- it is primarily designed as a user interface tool
-- and proceed to execute the `rm` and `cp` in the directory it happened to be
before.

Naturally, we may attempt to head off these cases with code that checks error
codes, attempts to recover, and so on. However, even the disciplined
programmer that leaves no value unturned must admit that this makes shell
scripts incomprehensible: `#!/bin/sh for attempt in 1 2 3 cd /work/foo if [ !
$? ] then echo "cd failed, trying again..." sleep 5 else break fi done if [ !
$? ] then echo "couldn't cd, giving up..." return 1 fi ` And that's just the
first line!

If we accept that failure, looping, timeouts, and job cancellation are
fundamental concerns in distributed systems, we may both simplify and
strengthen programs by making them fundamental expressions in a programming
language. These concepts are embodied in the simple `try` command:
`#!/usr/bin/ftsh try for 5 minutes every 30 seconds cd /work/foo rm -rf bar cp
-r /fresh/data . end `

Ftsh provides simple structures that encourage explicit acknowledgement of
failure while maintaining the readability of script code. You might think of
this as exceptions for scripts.

Want to learn more? This document is a short introduction to the fault
tolerant shell. It quickly breezes over the language features in order to get
started. You can learn more about the motivation for the language in ["The
Ethernet Approach to Grid
Computing"](http://ccl.cse.nd.edu/software/ftsh/ethernet-hpdc12.pdf),
available from the [ftsh web page](http://ccl.cse.nd.edu/software/ftsh) For a
quick introduction, read on!

## Basics⇗

### Simple Commands⇗

An ftsh program is built up from simple commands. A simple command names a
program to be executed, just `sh` or `csh`. The command is separated from its
arguments by whitespaces. Quotation marks may be used to escape whitespace.
For example: `ls -l /etc/hosts` or: `cat /etc/passwd` or: `cp "This File"
"That File"`

As you may know, a command (a UNIX process) returns an integer known as its
"exit code." Convention dictates that an exit code of zero indicates success
while any other number indicates failure. Languages tend to differ in their
mapping of integers to success or failure, so from here on, we will simply use
the abstract terms "success" and "failure."

A command may also fail in a variety of other ways without returning an exit
code. It may be killed by a signal, or it may fail to start altogether if the
program does not exist or its image cannot be loaded. These cases are also
considered failures.

### Groups⇗

A "group" is simply a list of commands. Each command only runs if the previous
command succeeded. Let's return our first example: `#!/usr/bin/ftsh cd
/work/foo rm -rf bar cp -r /fresh/data . ` This group succeeds only if **every
command** in the group succeeds. So, if ` cd` fails, then the whole group
fails and no further commands are executed.

This is called the "brittle" property of ftsh. If anything goes wrong, then
processing stops instantly. When something goes wrong, you will know it, and
the program will not "run away" executing more commands blindly. We will see
ways to contain the brittleness of a program below.

Ftsh itself has an exit code. Ftsh returns the result of the top-level group
that makes up the program. So, if any command in the top-level group fails,
then ftsh itself will fail. If they all succeed, then ftsh succeeds.

### Try Statements⇗

A try statement is used to contain and retry group failure. Here is a simple
try statement: `#!/usr/bin/ftsh try 5 times cd /work/foo rm -rf bar cp -r
/fresh/data . end ` The try statement attempts to execute the enclosed group
until the conditions in its header expire. Here, the group will be attempted
five times. Recall that a group fails as soon as any one command fails. So, if
`rm` fails, then the try statement will stop and attempt the group again from
the beginning.

If the five times are exceeded, then the try statement itself fails, and (if
it is the top-level try-statement) the whole shell program itself will fail.
If you prefer, you may catch and react to a try statement in a manner similar
to an exception. The `failure` keyword may be used to cause a new exception,
just like `throw` in other languages. `try 5 times cd /work/foo rm -rf bar cp
-r /fresh/data . catch echo "Oops, it failed. Oh well!" failure end `

Try statements come in several forms. They may limit the number of times the
group is executed. For example: `try for 10 times` A try statement may allow
an unlimited number of loops, terminated by a maximum amount of time, given in
seconds, minutes, hours, or days: `try for 45 seconds` Both may be combined,
yielding a try statement that stops when either the loop limit or the time
limit expires: `try for 3 days or 100 times` Note that such an statement does
not limit the length of any single attempt to execute the contained group. If
a single command is delayed for three days, the try statement will wait that
long and then kill the command. To force individual attempts to be shorter,
try statements may be nested. For example: `try for 3 days or 100 times try
for 1 time or 1 minute /bin/big-simulation end end ` Here, `big-simulation`
will be executed for no more than a minute at a time. Such one-minute attempts
will be tried for up to three days or one hundred attempts before the outer
try statement fails.

By default, ftsh uses an exponential backoff. If a group fails once, ftsh will
wait one second, and then try again. If it fails again, it will wait 2
seconds, then 4 seconds, and so on, doubling the waiting time after each
failure, up to a maximum of one hour. This prevents failures from consuming
excessive resources in fruitless retries.

If you prefer to have the retries occur at regular intervals (though we don't
recommend it) use the `every` keyword to control how frequently errors are
retried. For example: `try for 3 days every 1 hour try for 10 times every 30
seconds try for 1 minute or 3 times every 15 seconds ` If a time limit expires
in the middle of a try statement, then the currently running command is
forcibly cancelled. If an `every` clause is used, it merely ensures that each
attempt is at **least** that long. However, group will not be cancelled merely
to satisfy an ` every` clause. To ensure that a single loop attempt will be
time limited, you may combine two try statements as above: `try for 3 days or
100 times **every 1 minute** try for 1 time or **1 minute** /bin/big-
simulation end end ` Try statements themselves return either success or
failure in the same way as a simple command. If the enclosed group finally
succeeds, then the try expression itself succeeds. If the try expression
exhausts its attempts, then the try statement itself fails. We will make use
of this success or failure value in the next section.

Cancelling a process is somewhat more complicated than one might think. For
all the details on how this actually works, see the section on cancelling
processes below.

In (almost) all cases, a try statement absolutely controls what comes inside
of it. There are two ways for a subprogram to break out of the control of a
try. The first is to invoke ` exit`, which causes the entire ftsh process to
exit immediately with the given exit code. The second is to call `exec`, which
causes the given process to be run in place of the current shell process, thus
voiding any surrounding controls.

### Redirection⇗

Ftsh uses Bourne shell style I/O redirection. For example: `echo "hello" >
outfile` ...sends the output `hello` into the file `outfile`, likewise, ftsh
supports many of the more arcane redirections of the Bourne shell, such as the
redirection of explicit file descriptors: `grep needle 0<infile 1>outfile
2>errfile` ...appending to output files: `grep needle >>outfile 2>>errfile`
...redirection to an open file descriptor: `grep needle >outfile 2>&1` ...and
redirection of both input and output at once: `grep needle >& out-and-err-
file`

## Variables⇗

Ftsh provides variables similar to that of the Bourne shell. For example,
`name=Douglas echo "Hello, ${name}!" echo "Hello, $(name)!" echo "Hello,
$name!" ` Ftsh also allows variables to be the source and target of
redirections. That is, a variable may act as a file! The benefit of this
approach is that ftsh manages the storage and name space of variables for you.
You don't have to worry about cleaning up or clashing with other programs.

Variable redirection looks just like file redirection, except a dash is put in
front of the redirector. For example, For example, suppose that we want to
capture the output of `grep` and then run it through `sort`: `grep needle
/tmp/haystack -> needles sort -< needles ` This sort of operation takes the
place of a pipeline, which ftsh does not have (yet). However, by using
variables instead of pipelines, different retry conditions may be placed on
each stage of the work: `try for 5 times grep needle /tmp/haystack -> needles
end try for 1 hour sort -< needles end `

All of the variations on file redirection are available for variable
redirection, including -> and 2-> and ->> and 2->> and ->& and ->>&.

Like the Bourne shell, several variable names are reserved. Simple integers
are used to refer to the command line arguments given to ftsh itself. `$$`
names the current process. `$#` gives the number of arguments passed to the
program. `$*` gives all of the unquoted current arguments, while `"$@"` gives
all of the arguments individually quoted. The `shift` command can be used to
pop off the first positional argument.

Variables are implemented by creating temporary files and immediately
unlinking them after creation. Thus, no matter how ftsh exits \-- even if it
crashes -- the kernel deletes buffer space after you. This prevents both the
namespace and garbage collection problem left by scripts that manually read
and write to files.

## Structures⇗

Complex programs are built up by combining basic elements with programming
structures. Ftsh has most of the decision elements of other programming
languages, such as conditionals and loops. Each of these elements behaves in a
very precise way with respect to successes and failures.

### For-Statements⇗

A for-statement executes a command group once for each word in a list. For
example: `for food in bread wine meatballs echo "I like ${food}" end ` Of
course, the list of items may also come from a variable:
`packages="bread.tar.gz wine.tar.gz meatballs.tar.gz" for p in ${packages}
echo "Unpacking package ${p}..." tar xvzf ${p} end ` The more interesting
variations are `forany` and `forall`. A `forany` attempts to make a group
succeed once for any of the options given in the header, chosen randomly.
After the for-statement has run, the branch that succeeds in made available
through the control variable: `hosts="mirror1.wisc.edu mirror2.wisc.edu
mirror3.wisc.edu" forany h in ${hosts} echo "Attempting host ${host}" wget
http://${h}/some-file end echo "Got file from ${h}" ` A `forall` attempts to
make a group succeed for all of the options given in the header,
simultaneously: `forall h in ${hosts} ssh ${h} reboot end ` Both `for` and
`forall` are brittle with respect to failures. If any instance fails, then the
entire for-statement fails. A try-statement may be added in one of two ways.
If you wish to make each iteration resilient, place the try-statement inside
the for-statement: `for p in ${packages} try for 1 hour every 5 minutes echo
"Unpacking package ${p}..." tar xvzf ${p} end end ` Or, if you wish to make
the entire for-statement restart after a failure, place it outside: `try for 1
hour every 5 minutes for p in ${packages} echo "Unpacking package ${p}..." tar
xvzf ${p} end end `

### Loops, Conditionals, and Expressions⇗

Ftsh has loops and conditionals similar to other languages. For example: `n=0
while $n .lt. 10 echo "n is now ${n}" n=$n .add. 1 end ` And: `if $n .lt. 1000
echo "n is less than 1000" else if $n .eq. 1000 echo "n is equal to 1000" else
echo "n is greater than 1000" end ` You'll notice right away that arithmetic
expressions look a little different than other languages. Here's how it works:

The arithmetic operators .add. .sub. .mul. .div. .mod. .pow. represent
addition, subtraction, multiplication, division, modulus, and exponentiation,
including parenthesis and the usual order of operations. For example: `a=$x
.mul. ( $y .add. $z )` The comparison operators .eq. .ne. .le. .lt. .ge. .gt
represent equal, not-equal, less-than-or-equal, less-than, greater-than-or-
equal, and greater-than. These return the literal strings "true" and "false".
`uname -s -> n if $n .ne. Linux ... end ` For integer comparison, use the
operators .eql. and .neql.. `if $x .eql. 5 ... end ` The Boolean operators
.and. .or .not. have the usual meaning. An exception is thrown if they are
given arguments that are not the boolean strings "true" or "false". `while (
$x .lt. 10 ) .and. ( $y .gt. 20 ) ... end ` The unary file operators .exists.
.isr. .isw. .isx. test whether a filename exists, is readable, writeable, or
executable, respectively. The similar operators .isfile. .isdir. .issock.
.isfile. .isblock. .ischar. test for the type of a named file. All these
operators throw exceptions if the named file is unavailable for examination.
`f=/etc/passwd if ( .isfile. $f ) .and. ( .isr. $f ) ... end ` Finally, the
.to. and .step. operators are conveniences for generating numeric lists to be
used with for-loops: `forall x in 1 .to. 100 ssh c$x reboot end for x in 1
.to. 100 .step. 5 y=$x .mul. $x echo "$x times $x is $y" end ` Notice that,
unlike other shells, there is a distinction between expressions, which compute
a value or throw an exception, and external commands, which return no value.
Therefore, you cannot do this: `# !!! This is wrong !!! if rm $f echo "Removed
$f" else echo "Couldn't remove $f" end ` Instead, you want this: `try rm $f
echo "Removed $f" catch echo "Couldn't remove $f" end `

### Functions⇗

Simple functions are named groups of commands that may be called in the same
manner as an external program. The arguments passed to the function are
available in the same way as arguments to the shell: `function
compress_and_move echo "Working on ${1}..." gzip ${1} mv ${1}.gz ${2} end
compress_and_move /etc/hosts /tmp/hosts.gz compress_and_move /etc/passwd
/tmp/passwd.gz compress_and_move /usr/dict/words /tmp/dict.gz ` A function may
also be used to compute and return a value: `function fib if $1 .le. 1 return
1 else return fib($1 .sub. 1) .add. fib($1 .sub. 2) end end value=fib(100)
echo $value ` Functions, like groups, are brittle with respect to failures. A
failure inside a function causes the entire function to stop and fail
immediately. As in most languages, functions may be both nested and recursive.
However, ftsh aborts recursive function calls deeper than 1000 steps. If a
function is used in an expression but does not return a value, then the
expression evaluation fails.

## Miscellaneous Features⇗

### Environment⇗

Variables may be exported into the environment, just like the Bourne shell:
`PATH="/usr/bin:/usr/local/bin" export PATH `

### Nested Shells⇗

Ftsh is perfectly safe to nest. That is, an ftsh script may safely call other
scripts written in ftsh. One ftsh passes all of its options to sub-shells
using environment variables, so logs, error settings, and timeouts are uniform
from top to bottom. If a sub-shell provides its own arguments, these override
the environment settings of the parent.

### Error Logging⇗

Ftsh may optionally keep a log that describes all the details of a script's
execution. The -f option specifies a log file. Logs are open for appending, so
parallel and sub-shells may share the same log. The time, process number,
script, and line number are all recorded with every event.

**Note: Logs shared between processes must not be recorded in NFS or AFS
filesystems. NFS is not designed to support shared appending: your logs will
be corrupted sooner or later. AFS is not designed to support simultaneous
write sharing of a file: you will end up with the log of one process or
another, but not both. These are deliberate design limitations of these
filesystems and are not bugs in UNIX or ftsh.**

The amount of detail kept in a log is controled with the -l option. These
logging levels are currently defined:

  * **0** \- Nothing is logged. 
  * **10** \- Display failed commands and structures. 
  * **20** \- Display executed commands and their exit codes. 
  * **30** \- Display structural elements such as TRY and IF-THEN. 
  * **40** \- Display process activities such as signals and completions. 

### Command-Line Arguments⇗

Ftsh accepts the following command-line arguments:

  * **-f <file>** The name of a log file for tracing the execution of a script. This log file is opened in append mode. Equivalent to the environment variable FTSH_LOG_FILE. 
  * **-l <level>** The logging level, on a scale of 0 to 100. Higher numbers log more data about a script. Equivalent to the environment variable FTSH_LOG_LEVEL. 
  * **-k <mode>** \- Controls whether ftsh trusts the operating system to actually kill a process. If set to 'weak', ftsh will assume that processes die without checking. If set to 'strong', ftsh will issue SIGKILL repeatedly until a process actually dies. Equivalent to the environment variable FTSH_KILL_MODE. 
  * **-t <secs>** \- The amount of time ftsh will wait between requesting a process to exit (SIGTERM) and killing it forcibly (SIGKILL). Equivalent to the environment variable FTSH_KILL_TIMEOUT. 
  * **-p** Parse, but do not execute the script. This option may be used to test the validity of an Ftsh script. 
  * **-v** Show the version of ftsh. 
  * **-h** Show the help screen. 

### Cancelling Processes⇗

Cancelling a running process in UNIX is rather quite complex. Although
starting and stopping one single process is fairly easy, there are several
complications to manging a tree of processes, as well as dealing with the
various failures that can occur in the transmission of a signal.

Ftsh can clean up any set of processes that it starts, given the following
restrictions:

* Your programs must not create a new UNIX "process session" with the ` setsid()` system call. If you don't know what this is, then don't worry about it. 
* The operating system must actually kill a process when ftsh asks it to. Some variants of Linux won't kill processes using distributed file systems. Consider using the "weak" mode of ftsh. 
* Rogue system administrators must not forcibly kill an ftsh with a SIGKILL. However, you may safely send a SIGTERM, SIGHUP, SIGINT, or SIGQUIT to an ftsh, and it will clean up its children and exit. 

Ftsh starts every command as a separate UNIX process in its own process
session (i.e. `setsid`). This simplifies the administration of large process
tress. To cancel a command, ftsh sends a SIGTERM to every process in the
group. Ftsh then waits up to thirty seconds for the child to exit willingly.
At the end of that time, it forcibly terminates the entire process group with
a SIGKILL.

Surprisingly, SIGKILL is not always effective. Some operating systems have
bugs in which signals are occasionally lost or the process may be in such a
state that it cannot be killed at all. By default, ftsh tries very hard to
kill processes by issuing SIGKILL repeatedly until the process actually dies.
This is called the "strong" kill mode. If you do not wish to have this
behavior -- perhaps you have a bug resulting in unkillable processes -- then
you may run ftsh in the "weak" kill mode, using the "-k weak" option.

Ftsh may be safely nested. That is, an ftsh may invoke another program written
using ftsh. However, this child needs to clean up faster than its parents. If
the parent shell issues forcible kills after waiting for 30 seconds, then the
child must issue forcible kills before that. This problem is handled
transparently for you. Each ftsh informs its children of the current kill
timeout by setting the FTSH_KILL_TIMEOUT variable to five seconds less than
the current timeout. Thus, subshells are progressively less tolerant of
programs that refuse to exit cleanly.

* * *

