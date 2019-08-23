# Watchdog User's Manual

## Overview

Keeping a collection of processes running in large distributed system presents
many practical challenges. Machines reboot, programs crash, filesystems go up
and down, and software must be upgraded. Ensuring that a collection of
services is always running and up-to-date can require a large amount of manual
activity in the face of these challenges.

**watchdog** is a tool for keeping server processes running continuously. The
idea is simple: watchdog is responsible for starting a server. If the server
should crash or exit, watchdog restarts it. If the program on disk is
upgraded, watchdog will cleanly stop and restart the server to take advantage
of the new version. To avoid storms of coordinated activity in a large
cluster, these actions are taken with an exponential backoff and a random
factor.

**watchdog** is recommended for running the [chirp and catalog
servers](../chirp) found elsewhere in this package.


## Invocation

To run a server under the eye of watchdog, simply place ` watchdog` in front
of the server name.

That is, if you normally run:

```sh
chirp_server -r /my/data -p 10101
```

Then run this instead:

```sh
watchdog chirp_server -r /my/data -p 10101
```

For most situations, this is all that is needed. You may fine tune the behavior
of watchdog in the following ways:

**Logging.** Watchdog keeps a log of all actions that it performs on the
watched process. Use the ` -d all` option to see them, and the `-o file`
option to direct them to a log file:

```sh
# We use -- to separate the watchdog's options from the command to be executed:

watchdog -dall -o my.log -- chirp_server -r /my/data -p 10101
```

**Upgrading.** To upgrade servers running on a large cluster, simply install
the new binary in the filesystem. By default, each watchdog will check for an
upgraded binary once per hour and restart if necessary. Checks are randomly
distributed around the hour so that the network and/or filesystem will not be
overloaded. (To force a restart, send a SIGHUP to the watchdog.) Use the ` -c`
option to change the upgrade check interval.

**Timing.** Watchdog has several internal timers to ensure that the system is
not overloaded by cyclic errors. These can be adjusted by various options (in
parentheses.) A minimum time of ten seconds (`-m`) must pass between a server
stop and restart, regardless of the cause of the restart. If the server exits
within the first minute (`-s`) of execution, it is considered to have failed.
For each such failure, the minimum restart time is doubled, up to a maximum of
one hour (`-M`). If the program must be stopped, it is first sent an advisory
SIGTERM signal. If it does not exit voluntarily within one minute (`-S`), then
it is killed outright with a SIGKILL signal.


# Further Information

watchdog is Copyright (C) 2003-2004 Douglas Thain and Copyright (C) 2005- The
University of Notre Dame.  
All rights reserved.  
This software is distributed under the GNU General Public License.  
See the file COPYING for details.

Last edited: August 2019


