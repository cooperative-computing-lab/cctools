include(manual.h)dnl
HEADER(chirp)

SECTION(NAME)
BOLD(chirp) - command line tool providing explicit control of a Chirp server.

SECTION(SYNOPSIS)
CODE(BOLD(chirp [options] [hostname] [command]))

SECTION(DESCRIPTION)

BOLD(chirp) is a tool to connect and manage a Chirp server in a similar way to an FTP client.  BOLD(chirp) allows connecting to a Chirp server, copying files, and managing directories, auditing node etc...
PARA
Here are some important  BOLD(chirp) commands:

LIST_BEGIN
LIST_ITEM()BOLD(open) PARAM(host) Connect to a Chirp server.
LIST_ITEM()BOLD(close) Close connection to current Chirp server.  
LIST_ITEM()BOLD(get) PARAM(remotefile) [localfile] Copy a remote file to local storage.
LIST_ITEM()BOLD(put) PARAM(localfile) [remotefile] Copy a local file to Chirp server.
LIST_ITEM()BOLD(thirdput) PARAM(file) PARAM(3rdhost) PARAM(3rdfile) Copy a remote file to another Chirp server.
LIST_ITEM()BOLD(getacl) PARAM(remotepath) Get acl of a remote file/directory.
LIST_ITEM()BOLD(setacl) PARAM(remotepath) PARAM(user) PARAM(rwldax) Set acl for a remote file/directory.
LIST_ITEM()BOLD(ls) [-la] [remotepath] List contents of a remote directory.
LIST_ITEM()BOLD(mv) PARAM(oldname) PARAM(newname) Change name of a remote file.
LIST_ITEM()BOLD(rm) PARAM(file) Delete a remote file.
LIST_ITEM()BOLD(audit)	[-r] Audit current Chirp server.
LIST_ITEM()BOLD(exit) Close connection and exit BOLD(Chirp).
LIST_END

BOLD(chirp) also manages Chirp tickets for authentication purpose.

LIST_BEGIN
LIST_ITEM()BOLD(ticket_create) [-o[utput] PARAM(ticket filename)] [-s[ubject] PARAM(subject/user)] [-d[uration] PARAM(duration)] [-b[its] <bits>] [[PARAM(directory) PARAM(acl)] ...] Creat a ticket
LIST_ITEM()BOLD(ticket_register) PARAM(name) [PARAM(subject)] PARAM(duration) Manually register a ticket with multiple Chirp severs.
LIST_ITEM()BOLD(ticket_delete) PARAM(name) Remove a ticket.
LIST_ITEM()BOLD(ticket_list) PARAM(name) List registered tickets on a Chirp server.
LIST_ITEM()BOLD(ticket_get) PARAM(name) Check status of a ticket.
LIST_ITEM()BOLD(ticket_modify) PARAM(name) PARAM(directory) PARAM(aclmask) Modify a ticket.
LIST_END

SECTION(OPTIONS)
OPTIONS_BEGIN
OPTION_TRIPLET(-a, auth, flag)Require this authentication mode.
OPTION_TRIPLET(-d, debug, flag)Enable debugging for this subsystem.
OPTION_TRIPLET(-i, tickets, files)Comma-delimited list of tickets to use for authentication.
OPTION_ITEM(`-l, --verbose')Long transfer information.
OPTION_TRIPLET(-t, timeout, time)Set remote operation timeout.
OPTION_ITEM(`-v, --version')Show program version.
OPTION_ITEM(`-h, --help')Show help text.
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
% chirp server1.somewhere.edu
chirp> (enter more commands here)
LONGCODE_END

To copy a single local file using BOLD(chirp):

LONGCODE_BEGIN
% chirp server1.somewhere.edu put /tmp/mydata.dat /mydata/mydata.dat
LONGCODE_END

To get a single remote file using BOLD(chirp):

LONGCODE_BEGIN
% chirp server1.somewhere.edu get /mydata/mydata.dat /tmp/mydata.dat
LONGCODE_END

To create a ticket using:

LONGCODE_BEGIN
% chirp server1.somewhere.edu get ticket_create -output myticket.ticket -subject unix:user -bits 1024 -duration 86400 / rl /foo rwl 
LONGCODE_END

To register a ticket with other Chirp servers:

LONGCODE_BEGIN
% chirp server2.somewhere.edu ticket_register myticket.ticket unix:user 86400 
LONGCODE_END

To delete a ticket:

LONGCODE_BEGIN
% chirp server1.somewhere.edu ticket_delete myticket.ticket
LONGCODE_END


SECTION(COPYRIGHT)

COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)

SEE_ALSO_CHIRP

FOOTER

