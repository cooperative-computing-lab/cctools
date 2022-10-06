






















# catalog_query(1)

## NAME
**catalog_query** - query records from the catalog server

## SYNOPSIS
**catalog_query [--where [expr]] [--catalog [host]] [-d [flag]] [-o [file]] [-O [size]] [-t [timeout]] [-h] **

## DESCRIPTION

**catalog_query** is a tool that queries the catalog server for running services.
The output can be filtered by an arbitrary expression, and displayed in raw JSON
form, or in tabular form. This tool is handy for querying custom record types not handled
by other tools.

## ARGUMENTS


- **--where=_&lt;expr&gt;_**<br /> Only records matching this expression will be displayed.
- **--output=_&lt;expr&gt;_**<br /> Display this expression for each record.
- **--catalog=_&lt;host&gt;_**<br /> Query this catalog host.
- **--debug=_&lt;flag&gt;_**<br /> Enable debugging for this subsystem.
- **--debug-file=_&lt;file&gt;_**<br /> Send debug output to this file.
- **--debug-rotate-max=_&lt;bytes&gt;_**<br /> Rotate debug file once it reaches this size.
- **--timeout=_&lt;seconds&gt;_**<br /> Abandon the query after this many seconds.
- **--help**<br /> Show command options.


## EXAMPLES

To show all records in the catalog server:

```
% catalog_query
```

To show all records of other catalog servers:

```
% catalog_query --where \'type=="catalog"\'
```

To show all records of Chirp servers with more than 4 cpus:

```
% catalog_query --where \'type=="chirp" && cpus > 4\'
```

To show all records of WQ applications with name, port, and owner in tabular form:

```
% catalog_query --where \'type=="wq_master\" --output name --output port --output owner
```

To show all records of WQ applications with name, port, and owner as JSON records:

```
% catalog_query --where \'type=="wq_master\" --output '{"name":name,"port":port,"owner":owner}'
```

## COPYRIGHT

The Cooperative Computing Tools are Copyright (C) 2022 The University of Notre Dame.  This software is distributed under the GNU General Public License.  See the file COPYING for details.

## SEE ALSO

- [Cooperative Computing Tools Documentation]("../index.html")
- [catalog_server(1)](catalog_server.md)  [catalog_update(1)](catalog_update.md)  [catalog_query(1)](catalog_query.md)  [chirp_status(1)](chirp_status.md)  [work_queue_status(1)](work_queue_status.md)   [deltadb_query(1)](deltadb_query.md)


CCTools
