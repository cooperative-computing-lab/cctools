include(manual.h)dnl
HEADER(chirp_server_hdfs)

SECTION(NAME)
BOLD(chirp_server_hdfs) - run a chirp server with HDFS client setup

SECTION(SYNOPSIS)
CODE(BOLD(chirp_server_hdfs [options]))

SECTION(DESCRIPTION)

PARA
HDFS is the primary distributed filesystem used in the Hadoop project.
CODE(chirp_server_hdfs) enables running CODE(chirp_server) with the proper
environment variables set for using accessing HDFS. The command requires that
the CODE(JAVA_HOME) and CODE(HADOOP_HOME) environment variables be defined. Once
the environment is setup, the Chirp server will execute using HDFS as the backend
filesystem for all filesystem access.

PARA
For complete details with examples, see the LINK(Chirp User's Manual,http://www.nd.edu/~ccl/software/manuals/chirp.html).

SECTION(OPTIONS)

See MANPAGE(chirp_server,1) for option listing.

SECTION(ENVIRONMENT VARIABLES)

LIST_BEGIN
LIST_ITEM()CODE(BOLD(JAVA_HOME)) Location of your Java installation.
LIST_ITEM()CODE(BOLD(HADOOP_HOME)) Location of your Hadoop installation.
LIST_END


SECTION(EXIT STATUS)
On success, returns zero.  On failure, returns non-zero.

SECTION(EXAMPLES)

To start a Chirp server with a HDFS backend filesystem:

LONGCODE_BEGIN
% chirp_server_hdfs -r hdfs://host:port/foo/bar
LONGCODE_END

SECTION(COPYRIGHT)

COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)

SEE_ALSO_CHIRP

FOOTER
