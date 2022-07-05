






















# chirp_status(1)

## NAME
**chirp_status** - get current status of a one or more Chirp server(s)

## SYNOPSIS
**chirp_status [options] _&lt;nane&gt;_ _&lt;value&gt;_**

## DESCRIPTION
**chirp_status** is a tool for checking status of Chirp server(s).

**chirp_status** can look up Chirp server(s) using type, name, port, owner and version.

**chirp_status** by default lists type, name, port, owner, version, total and available storage of Chirp server(s)

When using **chirp_status** with long form option (-l), it lists additional information such as average load, available memory, operating system, up time, etc...

## OPTIONS

- **--where=_&lt;expr&gt;_**<br /> Show only servers matching this expression.
- **-c**,**--catalog=_&lt;host&gt;_**<br />Query the catalog on this host.
- **-A**,**--server-space=_&lt;size&gt;_**<br />Only show servers with this space available. (example: -A 100MB).
- **--server-project=_&lt;name&gt;_**<br />Only servers with this project name.
- **-t**,**--timeout=_&lt;time&gt;_**<br />Timeout.
- **-s**,**--brief**<br />Short output.
- **-l**,**--verbose**<br />Long output.
- **-T**,**--totals**<br />Totals output.
- **-v**,**--version**<br />Show program version.
- **-d**,**--debug=_&lt;flag&gt;_**<br />Enable debugging for this subsystem.
- **-o**,**--debug-file=_&lt;file&gt;_**<br />Write debugging output to this file. By default, debugging is sent to stderr (":stderr"). You may specify logs to be sent to stdout (":stdout") instead.
- **-O**,**--debug-rotate-max=_&lt;bytes&gt;_**<br />Rotate file once it reaches this size.
- **-h**,**--help**<br />Show help text.


## ENVIRONMENT VARIABLES


- **CHIRP_CLIENT_TICKETS** Comma delimited list of tickets to authenticate with (same as **-i**).


## EXIT STATUS
On success, returns zero.  On failure, returns non-zero.

## EXAMPLES

To show status of all available Chirp servers using **chirp_status**:

```
% chirp_status
```

To show status of a particular Chirp server:

```
% chirp_status --where 'name=="server1.somewhere.edu"'
```

To show status of Chirp servers which belong to a particular owner:

```
% chirp_status --where 'owner=="fred"'
```

To show all details in JSON format:

```
% chirp_status --long
```

To show aggregate status of all Chirp servers using  **chirp_status**:

```
% chirp_status -T
```

## COPYRIGHT

The Cooperative Computing Tools are Copyright (C) 2005-2022 The University of Notre Dame.  This software is distributed under the GNU General Public License.  See the file COPYING for details.

## SEE ALSO


- [Cooperative Computing Tools Documentation]("../index.html")
- [Chirp User Manual]("../chirp.html")
- [chirp(1)](chirp.md)  [chirp_status(1)](chirp_status.md)  [chirp_fuse(1)](chirp_fuse.md)  [chirp_get(1)](chirp_get.md)  [chirp_put(1)](chirp_put.md)  [chirp_stream_files(1)](chirp_stream_files.md)  [chirp_distribute(1)](chirp_distribute.md)  [chirp_benchmark(1)](chirp_benchmark.md)  [chirp_server(1)](chirp_server.md)


CCTools
