include(manual.h)dnl
HEADER(parrot_run_hdfs)dnl

SECTION(NAME)
BOLD(parrot_run_hdfs) - run a program in the Parrot virtual file system with HDFS client setup

SECTION(SYNOPSIS)
CODE(BOLD(parrot_run_hdfs [parrot_options] program [program_options]))

SECTION(DESCRIPTION)
PARA
CODE(parrot_run_hdfs) runs an application or a shell inside the Parrot virtual filesystem. 

PARA
HDFS is the primary distributed filesystem used in the Hadoop project. Parrot
supports read and write access to HDFS systems using the parrot_run_hdfs
wrapper. The command checks that the appropriate environmental variables are
defined and calls CODE(parrot_run). See MANPAGE(parrot_run,1).

PARA
In particular, you must ensure that you define the following environmental variables:

LIST_BEGIN
LIST_ITEM()CODE(BOLD(JAVA_HOME)) Location of your Java installation.
LIST_ITEM()CODE(BOLD(HADOOP_HOME)) Location of your Hadoop installation.
LIST_END

PARA
Based on these environmental variables, CODE(parrot_run_hdfs) will attempt to
find the appropriate paths for CODE(libjvm.so) and CODE(libhdfs.so). These
paths are stored in the environmental variables CODE(LIBJVM_PATH) and
CODE(LIBHDFS_PATH), which are used by the HDFS Parrot module to load the
necessary shared libraries at run-time. To avoid the startup overhead of
searching for these libraries, you may set the paths manually in your
environment before calling CODE(parrot_run_hdfs), or you may edit the script
directly.

PARA
Note that while Parrot supports read access to HDFS, it only provides
write-once support on HDFS. This is because the current implementations of HDFS
do not provide reliable append operations. Likewise, files can only be opened
in either read (O_RDONLY) or write mode (O_WRONLY), and not both (O_RDWR).

PARA
For complete details with examples, see the LINK(Parrot User's Manual,http://www.nd.edu/~ccl/software/manuals/parrot.html)

SECTION(EXIT STATUS)
CODE(parrot_run_hdfs) returns the exit status of the process that it runs.
If CODE(parrot_run_hdfs) is unable to start the process, it will return non-zero.

SECTION(EXAMPLES)
To access a single remote HDFS file using CODE(cat):
LONGCODE_BEGIN
parrot_run_hdfs cat /hdfs/server:port/foo
LONGCODE_END

You can also run an entire shell inside of Parrot, like this:
LONGCODE_BEGIN
parrot_run_hdfs bash
cd /hdfs
ls -la
cat foo
LONGCODE_END

SECTION(COPYRIGHT)

COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)

LIST_BEGIN
LIST_ITEM()MANPAGE(parrot_run,1)
LIST_ITEM()LINK(The Cooperative Computing Tools,http://www.nd.edu/~ccl/software/manuals)
LIST_ITEM()LINK(Parrot User's Manual,http://www.nd.edu/~ccl/software/manuals/parrot.html)
LIST_END
