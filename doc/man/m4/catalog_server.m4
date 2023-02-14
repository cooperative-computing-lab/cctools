include(manual.h)dnl
HEADER(catalog_server)

SECTION(NAME)
BOLD(catalog_server) - start a catalog server

SECTION(SYNOPSIS)
CODE(catalog_server [options])

SECTION(DESCRIPTION)

PARA
A catalog server provides naming and discovery for multiple components
of the Cooperative Computing Tools, particularly the Chirp distributed
filesystem and the Work Queue distributed programming framework.
Services that wish to be known on the network periodically publish
their information to the catalog server via a short TCP or UDP update.
Clients wishing to discover services by name may query the catalog
by issuing an HTTP request to the catalog server and will receive
back a listing of all known services.

PARA
To view the complete contents of the catalog, users can direct
their browser to CODE(http://catalog.cse.nd.edu:9097).  Command line tools
CODE(work_queue_status) and CODE(chirp_status) present the same data in
a form most useful for Work Queue and Chirp, respectively.
Large sites are encouraged
to run their own catalog server and set the CODE(CATALOG_HOST)
and CODE(CATALOG_PORT) environment variables to direct clients to their server.

PARA
The catalog server is a discovery service, not an authentication service,
so services are free to advertise whatever names and properties they please.
However, the catalog does update each incoming record with the actual IP address
and port from which it came, thus preventing a malicious service from
overwriting another service's record.

SECTION(OPTIONS)

OPTIONS_BEGIN
OPTION_FLAG(b,background)Run as a daemon.
OPTION_ARG(B, pid-file,file)Write process identifier (PID) to file.
OPTION_ARG(d, debug, flag)Enable debugging for this subsystem
OPTION_FLAG(h,help)Show this help screen
OPTION_ARG(H, history, directory) Store catalog history in this directory.  Enables fast data recovery after a failure or restart, and enables historical queries via deltadb_query.
OPTION_ARG(I, interface, addr)Listen only on this network interface.
OPTION_ARG(l, lifetime, secs)Lifetime of data, in seconds (default is 1800)
OPTION_ARG(L, update-log,file)Log new updates to this file.
OPTION_ARG(m, max-jobs,n)Maximum number of child processes.  (default is 50)
OPTION_ARG(M, server-size, size)Maximum size of a server to be believed.  (default is any)
OPTION_ARG(n, name, name)Set the preferred hostname of this server.
OPTION_ARG(o,debug-file,file)Write debugging output to this file. By default, debugging is sent to stderr (":stderr"). You may specify logs to be sent to stdout (":stdout") instead.
OPTION_ARG(O, debug-rotate-max, bytes)Rotate debug file once it reaches this size (default 10M, 0 disables).
OPTION_ARG(p,, port, port)Port number to listen on (default is 9097)
OPTION_FLAG(S,single)Single process mode; do not fork on queries.
OPTION_ARG(T, timeout, time)Maximum time to allow a query process to run.  (default is 60s)
OPTION_ARG(u, update-host, host)Send status updates to this host. (default is catalog.cse.nd.edu,backup-catalog.cse.nd.edu)
OPTION_ARG(U, update-interval, time)Send status updates at this interval. (default is 5m)
OPTION_FLAG(v,version)Show version string
OPTION_ARG(Z,port-file,file)Select port at random and write it to this file.  (default is disabled)
OPTIONS_END

SECTION(ENVIRONMENT VARIABLES)

LIST_BEGIN
LIST_ITEM(CODE(CATALOG_HOST)) Hostname of catalog server (same as CODE(-u)).
LIST_ITEM(CODE(CATALOG_PORT)) Port number of catalog server to be contacted.
LIST_ITEM(CODE(TCP_LOW_PORT)) Inclusive low port in range used with CODE(-Z).
LIST_ITEM(CODE(TCP_HIGH_PORT)) Inclusive high port in range used with CODE(-Z).
LIST_END

SECTION(EXIT STATUS)
On success, returns zero.  On failure, returns non-zero.

SECTION(COPYRIGHT)
COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)
SEE_ALSO_CATALOG

FOOTER
