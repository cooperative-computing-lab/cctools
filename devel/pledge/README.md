# Pledge

Tool to track what a program does to the filesystem.

## Building

We use `Make` as our build system, simply do:

```sh
make
```

And you're good to go.

### Colors

If you wish to have colored output, then you can run `make` like this instead:

```sh
make DEFS=-DCOLOR_ENFORCING
```

Do keep in mind that pipes sometimes have trouble dealing with colors.

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
Access       <Path>         Count
R            </usr/bin/cat> 1
M            </usr/lib/debug/libc.so.6> 1
M            </usr/lib/debug/> 1
MR           </etc/ld.so.cache> 2
MRW          </usr/lib/aarch64-linux-gnu/libc.so.6> 4
MR           </usr/lib/locale/locale-archive> 2
MR           </etc/locale.alias> 3
ME           </usr/lib/locale/C.UTF-8/LC_IDENTIFICATION> 1
MR           </usr/lib/locale/C.utf8/LC_IDENTIFICATION> 2
MR           </usr/lib/aarch64-linux-gnu/gconv/gconv-modules.cache> 2
MR           </usr/lib/locale/C.utf8/LC_MEASUREMENT> 2
MR           </usr/lib/locale/C.utf8/LC_TELEPHONE> 2
MR           </usr/lib/locale/C.utf8/LC_ADDRESS> 2
MR           </usr/lib/locale/C.utf8/LC_NAME> 2
MR           </usr/lib/locale/C.utf8/LC_PAPER> 2
M            </usr/lib/locale/C.utf8/LC_MESSAGES> 1
MR           </usr/lib/locale/C.utf8/LC_MESSAGES/SYS_LC_MESSAGES> 2
MR           </usr/lib/locale/C.utf8/LC_MONETARY> 2
MR           </usr/lib/locale/C.utf8/LC_COLLATE> 2
MR           </usr/lib/locale/C.utf8/LC_TIME> 2
MR           </usr/lib/locale/C.utf8/LC_NUMERIC> 2
MR           </usr/lib/locale/C.utf8/LC_CTYPE> 2
MR           </home/user/dummy/sample.c> 3
W            </dev/pts/0> 1

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
