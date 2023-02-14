# Catalog Server User's Manual

## Overview

Catalog servers function as connection points for tools that need to share
information and interact remotely. Various services and tools send periodic
updates to a catalog server to advertise their presence and vital details such
as addresses, resources, and performance. Tools like `chirp_status` and
`work_queue_status` query the server to displays servers that are currently
running. Catalog updates are sent via TCP or UDP, and the catalog server exposes a
JSON interface to view status and make queries.

By default, the cctools software makes use of the central catalog server (and
automatic backup) at Notre Dame:

[catalog.cse.nd.edu:9097](http://catalog.cse.nd.edu:9097)

[backup-catalog.cse.nd.edu:9097](http://backup-catalog.cse.nd.edu:9097)

The default view for a catalog server is a human-readable HTML summary.
Machine-readable data is also available as JSON, text, XML, or ClassAds. Many
parts of cctools make use of a catalog server internally. Chirp servers send
regular catalog updates indicating the host system's load, available disk
space, cctools version, etc. Work Queue managers also advertise their projects
through the catalog. When a worker starts, it can query the catalog to
automatically discover a manager to contact.

## Specifying Catalog Servers

Many of the tools accept command line arguments or environment variables to
specify the catalog server(s) to use. The catalog host is specified as a comma
delimited list of servers to use. Each may optionally include a port number.
If no port is specified, the value of the environment variable `CATALOG_PORT`
is used, or the default of port 9097. If no catalog server is given on the
command line, the `CATALOG_HOST` environment variable is used. If that is
unset, the default of `catalog.cse.nd.edu,backup-catalog.cse.nd.edu` This
could be written more verbosely as `catalog.cse.nd.edu:9097,backup-catalog.cse.nd.edu:9097` assuming the catalog port was not set in the
environment.

## Querying Catalog Servers

There are several ways to query a catalog server. If you are querying
specifically for Chirp servers or Work Queue applications, then use the
`chirp_status` or `work_queue_status` tools, which query the server and
display fields specific for those uses.

To view all kinds of records in raw JSON format, use the `catalog_query` tool.
This can be used to simply dump all records in JSON format:

```sh
catalog_query
```

Use the `--where` option to show only records matching an expression. (The
expression must be quoted to protect it from the shell.)  You may
construct expressions using any combination of operators and values in
the [JX Expression Language](../jx).

For example, to show all records of catalog servers:

```sh
catalog_query --where 'type=="catalog"'
```

Or to show all chirp servers with more than 4 cpus:

```sh
catalog_query --where 'type=="chirp" && cpus > 4'
```

The default output of `catalog_query` is raw JSON data, which is comprehensive,
but may contain much more than you need.  To limit the output, use one or
more `--output` options to select specific values to show.

```sh
catalog_query --where 'type=="chirp"' --output name --output port --output cpus
```

Again, you may construct complex output expressions using the [JX Expression Language](../jx):

```sh
catalog_query --where 'type=="chirp"' --output '{"name": name, "port": port, "size": avail/1024.0 }'
```

## Updating Catalog Servers

When any program is sending catalog updates, it will examine the environment
and/or configuration options to get a list of catalog servers in use. Updates
are then sent to every server listed. The program will consider it a success
if at least one update can be sent successfully. If DNS resolution fails for
every catalog server, for example, the program will report a failed update.

If you are constructing your own service, you can use the `catalog_update`
program to construct a custom message and send it to the catalog server. To do
so, create a file containing a valid JSON object with the desired properties,
and then run `catalog_update`. For example:

```json
cat > update.json << EOF
{
    "color" : "red",
    "active" : true,
    "size": 1200
}
EOF
```
```sh
catalog_update --catalog catalog.cse.nd.edu --file update.json
```

The `catalog_update` will insert into the object some additional basic
information about the node, such as the operating system, load average, and so
forth. When the update is received at the catalog server the name, address,
and port of the sender will be automatically overwritten, so it is not
possible to modify another machine's information.

These updates must be repeated on a regular basis, typically every 5 minutes,
in order to keep the catalog up to date. If an update is not received after 15
minutes, the entry is removed from the catalog.

Catalog updates are now able to be compressed, limiting the possibility of
packets being dropped enroute to the catalog. To enable this set the
environment variable **CATALOG_COMPRESS_UPDATES** to **on**.

Examples  
CSH:  
```csh
setenv CATALOG_COMPRESS_UPDATES on
```

Bash:  
```sh
export CATALOG_COMPRESS_UPDATES=on
```

By default, catalog updates are sent via the TCP protocol,
which is the most likely to succeed across various networking
environments.  However, it can cause brief delays while waiting
to connect to the catalog server.

If you have a large number of clients with frequent updates,
and are in the same networking domain as the catalog server,
you can alternatively send catalog updates via UDP packets instead.
To do this, set the following environment variable:
    
```sh
CATALOG_UPDATE_PROTOCOL=udp
```

(Prior to v7.4.16, the default was to send update via UDP.)

## Multiple Catalog Servers

When any of these tools are configured with multiple servers, the program will
try each in succession until receiving an answer. If no servers give valid
responses, the query as a whole fails. The order in which servers are listed
sets the initial query order. If a server fails to respond, it will be marked
as down before trying the next server in the list. On subsequent queries,
servers that were down will not be tried unless every other server is non-
responsive. If in this scenario the previously down server answers the query,
it will be marked as up again and used with normal priority in future queries.

## Running a Catalog Server

You may want to establish your own catalog server. This can be useful for
keeping your systems logically distinct from the main storage pool, but can
also help performance and availability if your catalog is close to your Chirp
servers. The catalog server is installed in the same place as the Chirp
server. Simply run it on any machine that you like and then direct your Chirp
servers to update the new catalog with the -u option. The catalog will be
published via HTTP on port 9097 of the catalog machine.

For example, suppose that you wish to run a catalog server on a machine named
`dopey` and a Chirp server on a machine named `sneezy`:

```sh
dopey$ catalog_server ...

sneezy$ chirp_server -u dopey [more options]
```

Finally, point your web browser to: `http://dopey:9097`

Or, set an environment variable and use Parrot:

```bash
$ export CATALOG_HOST=dopey
$ parrot_run bash
$ ls /chirp
```

And you will see [something like this.](http://catalog.cse.nd.edu:9097) You
may easily run multiple catalogs for either scalability or fault tolerance.
Simply give each Chirp server the name of each running catalog separated by
commas, e.g. `$ chirp_server -u 'dopey,happy:9000,grumpy'`

(Hint: If you want to ensure that your chirp and catalog servers run
continuously and are automatically restarted after an upgrade, consider using
[Watchdog](../watchdog).)

## Further Information

For more information, please see [Getting Help](../help) or visit the [Cooperative Computing Lab](http://ccl.cse.nd.edu) website.

## Copyright

CCTools is Copyright (C) 2022 The University of Notre Dame. This software is distributed under the GNU General Public License Version 2. See the file COPYING for
details.
