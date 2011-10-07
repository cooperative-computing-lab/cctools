include(manual.h)dnl
HEADER(chirp)

SECTION(NAME)
BOLD(chirp) - command line tool providing explicit control of a Chirp server.

SECTION(SYNOPSIS)
CODE(BOLD(chirp [options] [hostname] [command]))

SECTION(DESCRIPTION)

BOLD(chirp) is a tool to connect and manage a Chirp server in a similar way to an FTP client.
PARA
BOLD(chirp) allows connecting to a Chirp server, copying files, and managing directories, auditing node etc...
PARA
Here are some important  BOLD(chirp) commands:
PARA
BOLD(open) PARAM(host) Connect to a Chirp server.
PARA
BOLD(close) Close connection to current Chirp server.  
PARA
BOLD(get) PARAM(remotefile) [localfile] Copy a remote file to local storage.
PARA
BOLD(put) PARAM(localfile) [remotefile] Copy a local file to Chirp server.
PARA
BOLD(thirdput) PARAM(file) PARAM(3rdhost) PARAM(3rdfile) Copy a remote file to another Chirp server.
PARA
BOLD(getacl) PARAM(remotepath) Get acl of a remote file/directory.
PARA
BOLD(setacl) PARAM(remotepath) PARAM(user) PARAM(rwldax) Set acl for a remote file/directory.
PARA
BOLD(ls) [-la] [remotepath] List contents of a remote directory.
PARA
BOLD(mv) PARAM(oldname) PARAM(newname) Change name of a remote file.
PARA
BOLD(rm) PARAM(file) Delete a remote file.
PARA
BOLD(audit)	[-r] Audit current Chirp server.
PARA
BOLD(exit) Close connection and exit BOLD(Chirp).
PARA
PARA
BOLD(chirp) also manage Chirp tickets for authentication purpose.
PARA
BOLD(ticket_create) [-o[utput] PARAM(ticket filename)] [-s[ubject] PARAM(subject/user)] [-d[uration] PARAM(duration)] [-b[its] <bits>] [[PARAM(directory) PARAM(acl)] ...] Creat a ticket
PARA
BOLD(ticket_register) PARAM(name) [PARAM(subject)] PARAM(duration) Manually register a ticket with multiple Chirp severs.
PARA
BOLD(ticket_delete) PARAM(name) Remove a ticket.
PARA
BOLD(ticket_list) PARAM(name) List registered tickets on a Chirp server.
PARA
BOLD(ticket_get) PARAM(name) Check status of a ticket.
PARA
BOLD(ticket_modify) PARAM(name) PARAM(directory) PARAM(aclmask) Modify a ticket.

SECTION(OPTIONS)
OPTIONS_BEGIN
OPTION_PAIR(-a,mode) Require this authentication mode.
OPTION_PAIR(-d,subsystem) Enable debugging for this subsystem.
OPTION_ITEM(-h) Show help text.
OPTION_PAIR(-i,files) Comma-delimited list of tickets to use for authentication.
OPTION_ITEM(-l) Long transfer information.
OPTION_ITEM(-q) Quiet mode; supress messages and table headers.
OPTION_PAIR(-t,time) Set remote operation timeout.
OPTION_ITEM(-v) Show program version.
OPTIONS_END

SECTION(ENVIRONMENT VARIABLES)

LIST_BEGIN
LIST_ITEM()CODE(BOLD(CHIRP_CLIENT_TICKETS)) Comma delimited list of tickets to authenticate with (same as CODE(-i)).
LIST_END

SECTION(EXIT STATUS)
On success, returns zero.  On failure, returns non-zero.

SECTION(EXAMPLES)

To conenct to a Chirp server using BOLD(chirp):

LONGCODE_BEGIN
chirp server1.somewhere.edu
LONGCODE_END

To copy a single local file using BOLD(chirp):

LONGCODE_BEGIN
chirp server1.somewhere.edu put /tmp/mydata.dat /mydata/mydata.dat
LONGCODE_END

To get a single remote file using BOLD(chirp):

LONGCODE_BEGIN
chirp server1.somewhere.edu get /mydata/mydata.dat /tmp/mydata.dat
LONGCODE_END

To creat a ticket using:

LONGCODE_BEGIN
chirp server1.somewhere.edu get ticket_create -output myticket.ticket -subject unix:user -bits 1024 -duration 86400 / rl /foo rwl 
LONGCODE_END

To register a ticket with other Chirp servers:

LONGCODE_BEGIN
chirp server2.somewhere.edu ticket_register myticket.ticket unix:user 86400 
LONGCODE_END

To delete a ticket:

LONGCODE_BEGIN
chirp server1.somewhere.edu ticket_delete myticket.ticket
LONGCODE_END


SECTION(COPYRIGHT)

COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)

LIST_BEGIN
LIST_ITEM LINK(The Cooperative Computing Tools,"http://www.nd.edu/~ccl/software/manuals")
LIST_ITEM LINK(Parrot User Manual,"http://www.nd.edu/~ccl/software/manuals/parrot.html")
LIST_ITEM MANPAGE(parrot_run,1)
LIST_ITEM MANPAGE(makeflow,1)
LIST_END
