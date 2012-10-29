include(manual.h)dnl
HEADER(catalog_server)

SECTION(NAME)
BOLD(catalog_server) - start a catalog server

SECTION(SYNOPSIS)
CODE(BOLD(catalog_server [options]))

SECTION(DESCRIPTION)

PARA
A catalog server provides naming and discovery for multiple components
of the cooeprative computing tools, particularly the Chirp distributed
filesystem and the Work Queue distributed programming framework.
Services that wish to be known on the network periodically publish
their information to the catalog server via a short UDP packet.
Clients wishing to discover services by name may query the catalog
by issuing an HTTP request to the catalog server and will receive
back a listing of all known services.

PARA
To view the complete contents of the catalog, users can direct
their browser to CODE(http://chirp.cse.nd.edu:9097).  Command line tools
CODE(work_queue_status) and CODE(chirp_status) present the same data in
a form most useful for Work Queue and Chirp, respectively.
Large sites are encouraged
to run their own catalog server and set the CODE(CATALOG_HOST) environment
to direct clients to their server.

PARA
The catalog server is a discovery service, not an authentication service,
so services are free to advertise whatever names and properties they please.
However, it does update each incoming record with the actual IP address
and port from which it came, thus preventing a malicious service from
overwriting another service's record.

SECTION(OPTIONS)

OPTIONS_BEGIN
OPTION_ITEM(-b)           Run as a daemon.
OPTION_PAIR(-B,file)      Write process identifier (PID) to file.
OPTION_PAIR(-p,port)      Port number to listen on (default is 9097)
OPTION_PAIR(-l,secs)      Lifetime of data, in seconds (default is 1800)
OPTION_PAIR(-d,subsystem) Enable debugging for this subsystem
OPTION_PAIR(-o,file)      Send debugging to this file.
OPTION_PAIR(-O,bytes)     Rotate debug file once it reaches this size.
OPTION_PAIR(-u,host)      Send status updates to this host. (default is chirp.cse.nd.edu)
OPTION_PAIR(-m,n)         Maximum number of child processes.  (default is 50)
OPTION_PAIR(-T,time)      Maximum time to allow a query process to run.  (default is 60s)
OPTION_PAIR(-M,size)      Maximum size of a server to be believed.  (default is any)
OPTION_PAIR(-U,time)      Send status updates at this interval. (default is 5m)
OPTION_PAIR(-L,file)      Log new updates to this file.
OPTION_ITEM(-S)             Single process mode; do not work on queries.
OPTION_ITEM(-v)             Show version string
OPTION_ITEM(-h)             Show this help screen
OPTIONS_END

SECTION(ENVIRONMENT VARIABLES)

LIST_BEGIN
LIST_ITEM()CODE(BOLD(CATALOG_HOST)) Hostname of catalog server (same as CODE(-u)).
LIST_ITEM()CODE(BOLD(TCP_LOW_PORT)) Inclusive low port in range used with CODE(-Z).
LIST_ITEM()CODE(BOLD(TCP_HIGH_PORT)) Inclusive high port in range used with CODE(-Z).
LIST_END

SECTION(EXIT STATUS)
On success, returns zero.  On failure, returns non-zero.

SECTION(COPYRIGHT)
COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)
SEE_ALSO_CHIRP

FOOTER

