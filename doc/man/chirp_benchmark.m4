include(manual.h)dnl
HEADER(chirp_benchmark)

SECTION(NAME)
BOLD(chirp_benchmark) - do micro-performance tests on a Chirp server

SECTION(SYNOPSIS)
CODE(BOLD(chirp_benchmark PARAM(host[:port]) PARAM(file) PARAM(loops) PARAM(cycles) PARAM(bwloops)))

SECTION(DESCRIPTION)

PARA
CODE(chirp_benchmark) tests a Chirp server's bandwidth and latency for various
Remote Procedure Calls. The command uses a combination of RPCs that do and do
not use I/O bandwidth on the backend filesystem to measure the latency. It also
tests the throughput for reading and writing to the given filename with
various block sizes.

PARA
For complete details with examples, see the LINK(Chirp User's Manual,http://www.nd.edu/~ccl/software/manuals/chirp.html).

SECTION(EXIT STATUS)
On success, returns zero.  On failure, returns non-zero.

SECTION(EXAMPLES)

To test a Chirp server, you may use:

LONGCODE_BEGIN
$ chirp_benchmark host:port foobar 10 10 10
getpid     1.0000 +/-    0.1414  usec
write1   496.3200 +/-   41.1547  usec
write8   640.0400 +/-   23.8790  usec
read1     41.0400 +/-   91.3210  usec
read8      0.9200 +/-    0.1789  usec
stat     530.2400 +/-   14.2425  usec
open    1048.1200 +/-   15.5097  usec
...
LONGCODE_END

SECTION(COPYRIGHT)

COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)

SEE_ALSO_CHIRP

FOOTER
