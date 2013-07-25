include(manual.h)dnl
HEADER(chirp_stream_files)

SECTION(NAME)
BOLD(chirp_stream_files) - move data to/from chirp servers in parallel

SECTION(SYNOPSIS)
CODE(BOLD(chirp_stream_files [options] PARAM(copy|split|join) PARAM(localfile) { PARAM(hostname[:port]) PARAM(remotefile)))

SECTION(DESCRIPTION)

BOLD(chirp_stream_files) is a tool for moving data from one machine to and from many machines, with the option to split or join the file along the way.  It is useful for constructing scatter-gather types of applications on top of Chirp.
PARA
CODE(chirp_stream_files copy) duplicates a single file to multiple hosts.  The PARAM(localfile) argument names a file on the local filesystem.  The command will then open a connection to the following list of hosts, and stream the file to all simultaneously.
PARA
CODE(chirp_stream_files split) divides an ASCII file up among multiple hosts.  The first line of PARAM(localfile) is sent to the first host, the second line to the second, and so on, round-robin.
PARA
CODE(chirp_stream_files join) collects multiple remote files into one.  The argument PARAM(localfile) is opened for writing, and the remote files for reading.  The remote files are read line-by-line and assembled round-robin into the local file.
PARA
In all cases, files are accessed in a streaming manner, making this particularly efficient for processing large files.  A local file name of CODE(-) indicates standard input or standard output, so that the command can be used in a pipeline.
SECTION(OPTIONS)

OPTIONS_BEGIN
OPTION_TRIPLET(-a, auth,flag)Require this authentication mode.
OPTION_TRIPLET(-b,block-size,size)Set transfer buffer size. (default is 1048576 bytes)
OPTION_TRIPLET(-d,debug,flag)Enable debugging for this subsystem.
OPTION_TRIPLET(-i,tickes,files)Comma-delimited list of tickets to use for authentication.
OPTION_TRIPLET(-t,timeout,time)Timeout for failure. (default is 3600s)
OPTION_ITEM(`-v, --version')Show program version.
OPTION_ITEM(`-h, --help')Show help text.
OPTIONS_END

SECTION(ENVIRONMENT VARIABLES)
List any environment variables used or set in this section.

SECTION(EXIT STATUS)
On success, returns zero.  On failure, returns non-zero.

SECTION(EXAMPLES)

To copy the file CODE(mydata) to three locations:

LONGCODE_BEGIN
% chirp_stream_files copy mydata server1.somewhere.edu /mydata
                                 server2.somewhere.edu /mydata
                                 server2.somewhere.edu /mydata
LONGCODE_END

To split the file CODE(mydata) into subsets at three locations:

LONGCODE_BEGIN
% chirp_stream_files split mydata server1.somewhere.edu /part1
                                  server2.somewhere.edu /part2
                                  server2.somewhere.edu /part3
LONGCODE_END

To join three remote files back into one called CODE(newdata):

LONGCODE_BEGIN
% chirp_stream_files join newdata server1.somewhere.edu /part1
                                  server2.somewhere.edu /part2
                                  server2.somewhere.edu /part3
LONGCODE_END

SECTION(COPYRIGHT)

COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)

SEE_ALSO_CHIRP

FOOTER

