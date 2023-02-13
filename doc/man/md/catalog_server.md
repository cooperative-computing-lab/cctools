






















# catalog_server(1)

## NAME
**catalog_server** - start a catalog server

## SYNOPSIS
**catalog_server [options]**

## DESCRIPTION


A catalog server provides naming and discovery for multiple components
of the Cooperative Computing Tools, particularly the Chirp distributed
filesystem and the Work Queue distributed programming framework.
Services that wish to be known on the network periodically publish
their information to the catalog server via a short TCP or UDP update.
Clients wishing to discover services by name may query the catalog
by issuing an HTTP request to the catalog server and will receive
back a listing of all known services.


To view the complete contents of the catalog, users can direct
their browser to **http://catalog.cse.nd.edu:9097**.  Command line tools
**work_queue_status** and **chirp_status** present the same data in
a form most useful for Work Queue and Chirp, respectively.
Large sites are encouraged
to run their own catalog server and set the **CATALOG_HOST**
and **CATALOG_PORT** environment variables to direct clients to their server.


The catalog server is a discovery service, not an authentication service,
so services are free to advertise whatever names and properties they please.
However, the catalog does update each incoming record with the actual IP address
and port from which it came, thus preventing a malicious service from
overwriting another service's record.

## OPTIONS


- **-b**,**--background**<br />Run as a daemon.
- **-B**,**--pid-file=_&lt;file&gt;_**<br />Write process identifier (PID) to file.
- **-d**,**--debug=_&lt;flag&gt;_**<br />Enable debugging for this subsystem
- **-h**,**--help**<br />Show this help screen
- **-H**,**--history=_&lt;directory&gt;_**<br /> Store catalog history in this directory.  Enables fast data recovery after a failure or restart, and enables historical queries via deltadb_query.
- **-I**,**--interface=_&lt;addr&gt;_**<br />Listen only on this network interface.
- **-l**,**--lifetime=_&lt;secs&gt;_**<br />Lifetime of data, in seconds (default is 1800)
- **-L**,**--update-log=_&lt;file&gt;_**<br />Log new updates to this file.
- **-m**,**--max-jobs=_&lt;n&gt;_**<br />Maximum number of child processes.  (default is 50)
- **-M**,**--server-size=_&lt;size&gt;_**<br />Maximum size of a server to be believed.  (default is any)
- **-n**,**--name=_&lt;name&gt;_**<br />Set the preferred hostname of this server.
- **-o**,**--debug-file=_&lt;file&gt;_**<br />Write debugging output to this file. By default, debugging is sent to stderr (":stderr"). You may specify logs to be sent to stdout (":stdout") instead.
- **-O**,**--debug-rotate-max=_&lt;bytes&gt;_**<br />Rotate debug file once it reaches this size (default 10M, 0 disables).
- **-p**,**--=_&lt;port&gt;_**<br />Port number to listen on (default is 9097)
- **-S**,**--single**<br />Single process mode; do not fork on queries.
- **-T**,**--timeout=_&lt;time&gt;_**<br />Maximum time to allow a query process to run.  (default is 60s)
- **-u**,**--update-host=_&lt;host&gt;_**<br />Send status updates to this host. (default is catalog.cse.nd.edu,backup-catalog.cse.nd.edu)
- **-U**,**--update-interval=_&lt;time&gt;_**<br />Send status updates at this interval. (default is 5m)
- **-v**,**--version**<br />Show version string
- **-Z**,**--port-file=_&lt;file&gt;_**<br />Select port at random and write it to this file.  (default is disabled)


## ENVIRONMENT VARIABLES


- **CATALOG_HOST** Hostname of catalog server (same as **-u**).
- **CATALOG_PORT** Port number of catalog server to be contacted.
- **TCP_LOW_PORT** Inclusive low port in range used with **-Z**.
- **TCP_HIGH_PORT** Inclusive high port in range used with **-Z**.


## EXIT STATUS
On success, returns zero.  On failure, returns non-zero.

## COPYRIGHT
The Cooperative Computing Tools are Copyright (C) 2022 The University of Notre Dame.  This software is distributed under the GNU General Public License.  See the file COPYING for details.

## SEE ALSO

- [Cooperative Computing Tools Documentation]("../index.html")
- [catalog_server(1)](catalog_server.md)  [catalog_update(1)](catalog_update.md)  [catalog_query(1)](catalog_query.md)  [chirp_status(1)](chirp_status.md)  [work_queue_status(1)](work_queue_status.md)   [deltadb_query(1)](deltadb_query.md)


CCTools
