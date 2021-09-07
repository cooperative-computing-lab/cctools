






















# chirp(1)

## NAME
**chirp** - command line tool providing explicit control of a Chirp server.

## SYNOPSIS
****chirp [options] [hostname] [command]****

## DESCRIPTION

**chirp** is a tool to connect and manage a Chirp server in a similar way to an FTP client.  **chirp** allows connecting to a Chirp server, copying files, and managing directories, auditing node etc...

Here are some important  **chirp** commands:


- **open** <host> Connect to a Chirp server.
- **close** Close connection to current Chirp server.  
- **get** <remotefile> [localfile] Copy a remote file to local storage.
- **put** <localfile> [remotefile] Copy a local file to Chirp server.
- **thirdput** <file> <3rdhost> <3rdfile> Copy a remote file to another Chirp server.
- **getacl** <remotepath> Get acl of a remote file/directory.
- **setacl** <remotepath> <user> <rwldax> Set acl for a remote file/directory.
- **ls** [-la] [remotepath] List contents of a remote directory.
- **mv** <oldname> <newname> Change name of a remote file.
- **rm** <file> Delete a remote file.
- **audit**	[-r] Audit current Chirp server.
- **exit** Close connection and exit **Chirp**.


**chirp** also manages Chirp tickets for authentication purpose.


- **ticket_create** [-o[utput] <ticket filename>] [-s[ubject] <subject/user>] [-d[uration] <duration>] [-b[its] <bits>] [[<directory> <acl>] ...] Creat a ticket
- **ticket_register** <name> [<subject>] <duration> Manually register a ticket with multiple Chirp severs.
- **ticket_delete** <name> Remove a ticket.
- **ticket_list** <name> List registered tickets on a Chirp server.
- **ticket_get** <name> Check status of a ticket.
- **ticket_modify** <name> <directory> <aclmask> Modify a ticket.


## OPTIONS

- **-a --auth <flag>** Require this authentication mode.
- **-d --debug <flag>** Enable debugging for this subsystem.
- **-i --tickets <files>** Comma-delimited list of tickets to use for authentication.
- **-l, --verbose** Long transfer information.
- **-t --timeout <time>** Set remote operation timeout.
- **-v, --version** Show program version.
- **-h, --help** Show help text.


## ENVIRONMENT VARIABLES


- ****CHIRP_CLIENT_TICKETS**** Comma delimited list of tickets to authenticate with (same as **-i**).


## EXIT STATUS
On success, returns zero.  On failure, returns non-zero.

## EXAMPLES

To connect to a Chirp server using **chirp**:

```
% chirp server1.somewhere.edu
chirp> (enter more commands here)
```

To copy a single local file using **chirp**:

```
% chirp server1.somewhere.edu put /tmp/mydata.dat /mydata/mydata.dat
```

To get a single remote file using **chirp**:

```
% chirp server1.somewhere.edu get /mydata/mydata.dat /tmp/mydata.dat
```

To create a ticket using:

```
% chirp server1.somewhere.edu get ticket_create -output myticket.ticket -subject unix:user -bits 1024 -duration 86400 / rl /foo rwl
```

To register a ticket with other Chirp servers:

```
% chirp server2.somewhere.edu ticket_register myticket.ticket unix:user 86400
```

To delete a ticket:

```
% chirp server1.somewhere.edu ticket_delete myticket.ticket
```


## COPYRIGHT

The Cooperative Computing Tools are Copyright (C) 2005-2021 The University of Notre Dame.  This software is distributed under the GNU General Public License.  See the file COPYING for details.

## SEE ALSO


- [Cooperative Computing Tools Documentation]("../index.html")
- [Chirp User Manual]("../chirp.html")
- [chirp(1)](chirp.md)  [chirp_status(1)](chirp_status.md)  [chirp_fuse(1)](chirp_fuse.md)  [chirp_get(1)](chirp_get.md)  [chirp_put(1)](chirp_put.md)  [chirp_stream_files(1)](chirp_stream_files.md)  [chirp_distribute(1)](chirp_distribute.md)  [chirp_benchmark(1)](chirp_benchmark.md)  [chirp_server(1)](chirp_server.md)


CCTools 8.0.0 DEVELOPMENT
