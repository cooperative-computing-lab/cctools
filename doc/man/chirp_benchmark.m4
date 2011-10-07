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
16384 write       78.2745 +/-   11.4450  MB/s
16384 read        81.7679 +/-    8.2247  MB/s
8192 write        94.6223 +/-    0.1901  MB/s
8192 read         96.1256 +/-    0.3295  MB/s
4096 write        99.6112 +/-    1.0009  MB/s
4096 read        101.8522 +/-    0.5334  MB/s
2048 write       101.2376 +/-    0.7628  MB/s
2048 read        104.3760 +/-    0.3871  MB/s
1024 write        99.9348 +/-    0.8109  MB/s
1024 read        103.2432 +/-    0.1942  MB/s
512 write        94.3956 +/-    0.6388  MB/s
512 read         97.6443 +/-    1.8272  MB/s
256 write        83.2254 +/-    1.3467  MB/s
256 read         89.7202 +/-    0.6206  MB/s
128 write        70.6562 +/-    0.8749  MB/s
128 read         75.3090 +/-    0.1642  MB/s
64 write        48.5974 +/-    0.3102  MB/s
64 read         51.1825 +/-    0.5314  MB/s
32 write        48.8378 +/-    0.4145  MB/s
32 read         51.1169 +/-    0.3965  MB/s
16 write        48.1118 +/-    0.2769  MB/s
16 read         50.9808 +/-    0.4732  MB/s
8 write        48.6532 +/-    0.6860  MB/s
8 read         50.9466 +/-    0.3846  MB/s
4 write        48.3652 +/-    0.6916  MB/s
4 read         50.9254 +/-    0.3587  MB/s
LONGCODE_END

SECTION(COPYRIGHT)

COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)

LIST_BEGIN
LIST_ITEM LINK(The Cooperative Computing Tools,"http://www.nd.edu/~ccl/software/manuals")
LIST_ITEM LINK(Chirp User Manual,"http://www.nd.edu/~ccl/software/manuals/chirp.html")
LIST_ITEM MANPAGE(chirp,1),MANPAGE(chirp_server,1)
LIST_END
