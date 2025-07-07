# Pledge

Tool to track what a program does to the filesystem.

## Building

### CMake

To build with CMake do:

```sh
cmake -B build -G Ninja
ninja -C build
```

### Make

To build with Make do:

```sh
make
```

## Using

Pledge has 2 tools inside of it, the tracer and the enforcer.

### Tracer

The tracer is ran like this:

```sh
pledge trace cat sample.c
```

It's output should be something like this:

```
Tracer [cat.sample.c]: Strace log generated -> cat.sample.c.tracer.log
Tracer [cat.sample.c]: Contract generated   -> cat.sample.c.contract
```

The most important file here is the contract file, if we `cat` it we get:

```
action        path
R             /home/user/dummy/cat.sample.c.contract
R             /usr/bin/cat
R             /home/user/dummy
R             /etc/ld.so.cache
R             /usr/lib/aarch64-linux-gnu/libc.so.6
R             /usr/lib/locale/locale-archive
R             /etc/locale.alias
R             /usr/lib/locale/C.utf8/LC_IDENTIFICATION
R             /usr/lib/aarch64-linux-gnu/gconv/gconv-modules.cache
R             /usr/lib/locale/C.utf8/LC_MEASUREMENT
R             /usr/lib/locale/C.utf8/LC_TELEPHONE
R             /usr/lib/locale/C.utf8/LC_ADDRESS
R             /usr/lib/locale/C.utf8/LC_NAME
R             /usr/lib/locale/C.utf8/LC_PAPER
R             /usr/lib/locale/C.utf8/LC_MESSAGES
R             /usr/lib/locale/C.utf8/LC_MESSAGES/SYS_LC_MESSAGES
R             /usr/lib/locale/C.utf8/LC_MONETARY
R             /usr/lib/locale/C.utf8/LC_COLLATE
R             /usr/lib/locale/C.utf8/LC_TIME
R             /usr/lib/locale/C.utf8/LC_NUMERIC
R             /usr/lib/locale/C.utf8/LC_CTYPE
R             /home/user/dummy/sample.c
```

### Enforcer

Now that we have a contract we can use the enforcer.
We first have to set the environment variable:<br>
`export CONTRACT=./cat.sample.c.contract`<br>
We use `LD_PRELOAD`, however, `PLEDGE` temporarily writes an `.so` called `minienforcer.so` and appends its path to the `LD_PRELOAD` environment variable, so the user does not have to set it.<br>
We can run our command with:<br>
`pledge enforce cat sample.c`<br>
The output should be something like this:<br>

```
Enforcer path: /home/user/dummy/cat.sample.c.contract
OPEN: caught open with path [sample.c]
with absolute [/home/user/dummy/sample.c]
ALLOWED: Path [/home/user/dummy/sample.c] with permission [R] is not in violation of the contract.
READING: caught path [/proc/self/fd/3] with link to [/home/user/dummy/sample.c]
ALLOWED: Path [/home/user/dummy/sample.c] with permission [R] is not in violation of the contract.
WRITING: caught path [/proc/self/fd/1] with link to [/dev/pts/0]
WHITELISTED: Path [/dev/pts/0] is whitelisted internally.
#include <stdio.h>

void
main()
{
    printf("Meow!");
}
READING: caught path [/proc/self/fd/3] with link to [/home/user/dummy/sample.c]
ALLOWED: Path [/home/user/dummy/sample.c] with permission [R] is not in violation of the contract.
```
