






















# chirp(1)

## NAME
**chirp** - command line tool providing explicit control of a Chirp server.

## SYNOPSIS
**chirp [options] [hostname] [command]**

## DESCRIPTION

**chirp** is a tool to connect and manage a Chirp server in a similar way to an FTP client.  **chirp** allows connecting to a Chirp server, copying files, and managing directories, auditing node etc...

Here are some important  **chirp** commands:


- **open** _&lt;host&gt;_ Connect to a Chirp server.
- **close** Close connection to current Chirp server.  
- **get** _&lt;remotefile&gt;_ [localfile] Copy a remote file to local storage.
- **put** _&lt;localfile&gt;_ [remotefile] Copy a local file to Chirp server.
- **thirdput** _&lt;file&gt;_ _&lt;3rdhost&gt;_ _&lt;3rdfile&gt;_ Copy a remote file to another Chirp server.
- **getacl** _&lt;remotepath&gt;_ Get acl of a remote file/directory.
- **setacl** _&lt;remotepath&gt;_ _&lt;user&gt;_ _&lt;rwldax&gt;_ Set acl for a remote file/directory.
- **ls** [-la] [remotepath] List contents of a remote directory.
- **mv** _&lt;oldname&gt;_ _&lt;newname&gt;_ Change name of a remote file.
- **rm** _&lt;file&gt;_ Delete a remote file.
- **audit**	[-r] Audit current Chirp server.
- **exit** Close connection and exit **Chirp**.


**chirp** also manages Chirp tickets for authentication purpose.


- **ticket_create** [-o[utput] _&lt;ticket filename&gt;_] [-s[ubject] _&lt;subject/user&gt;_] [-d[uration] _&lt;duration&gt;_] [-b[its] _&lt;bits&gt;_] [[_&lt;directory&gt;_ _&lt;acl&gt;_] ...] Creat a ticket
- **ticket_create** [-o[utput] _&lt;ticket filename&gt;_] [-s[ubject] _&lt;subject/user&gt;_] [-d[uration] _&lt;duration&gt;_] [-b[its] _&lt;bits&gt;_] [[_&lt;directory&gt;_ _&lt;acl&gt;_] ...] Creat a ticket
- **ticket_register** _&lt;name&gt;_ [_&lt;subject&gt;_] _&lt;duration&gt;_ Manually register a ticket with multiple Chirp severs.
- **ticket_delete** _&lt;name&gt;_ Remove a ticket.
- **ticket_list** _&lt;name&gt;_ List registered tickets on a Chirp server.
- **ticket_get** _&lt;name&gt;_ Check status of a ticket.
- **ticket_modify** _&lt;name&gt;_ _&lt;directory&gt;_ _&lt;aclmask&gt;_ Modify a ticket.


## OPTIONS

- **-a**,**--auth=_&lt;flag&gt;_**<br /> Enable authentication mode: unix, hostname, address, ticket, kerberos, or globus.
- **-d**,**--debug=_&lt;flag&gt;_**<br />Enable debugging for this subsystem.
- **-i**,**--tickets=_&lt;files&gt;_**<br />Comma-delimited list of tickets to use for authentication.
- **-l**,**--verbose**<br />Long transfer information.
- **-t**,**--timeout=_&lt;time&gt;_**<br />Set remote operation timeout.
- **-v**,**--version**<br />Show program version.
- **-h**,**--help**<br />Show help text.


## ENVIRONMENT VARIABLES


- **CHIRP_CLIENT_TICKETS** Comma delimited list of tickets to authenticate with (same as **-i**).


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

The Cooperative Computing Tools are Copyright (C) 2022 The University of Notre Dame.  This software is distributed under the GNU General Public License.  See the file COPYING for details.

## SEE ALSO


- [Cooperative Computing Tools Documentation]("../index.html")
- [Chirp User Manual]("../chirp.html")
- [chirp(1)](chirp.md)  [chirp_status(1)](chirp_status.md)  [chirp_fuse(1)](chirp_fuse.md)  [chirp_get(1)](chirp_get.md)  [chirp_put(1)](chirp_put.md)  [chirp_stream_files(1)](chirp_stream_files.md)  [chirp_distribute(1)](chirp_distribute.md)  [chirp_benchmark(1)](chirp_benchmark.md)  [chirp_server(1)](chirp_server.md)


CCTools
