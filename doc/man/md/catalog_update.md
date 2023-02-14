






















# catalog_update(1)

## NAME
**catalog_update** - send update to catalog server

## SYNOPSIS
**catalog_update [options] [name=value] ..**

## DESCRIPTION


The **catalog_update** tool allows users to manually send an update to a
catalog server via TCP or UDP.

## OPTIONS


- **-c**,**--catalog=_&lt;host&gt;_**<br />Send update to this catalog host.
- **-f**,**--file=_&lt;json-file&gt;_**<br /> Send additional JSON attributes in this file.
- **-d**,**--debug=_&lt;flags&gt;_**<br /> Enable debug flags.
- **-o**,**--debug-file=_&lt;file&gt;_**<br /> Send debug output to this file.
- **-v** _&lt;version&gt;_<br /> Show software version.
- **-h** _&lt;help&gt;_<br /> Show all options.


The **catalog_update** tool sends a custom message to the catalog
server in the from of a JSON object with various properties describing
the host.  By default, the **catalog_update** tool includes the following
fields in the update:


- **type** This describes the node type (default is "node").
- **version** This is the version of CCTools.
- **cpu** This is CPU architecture of the machine.
- **opsys** This is operating system of the machine.
- **opsysversion** This is operating system version of the machine.
- **load1** This is 1-minute load of the machine.
- **load5** This is 5-minute load of the machine.
- **load15** This is 15-minute load of the machine.
- **memory_total** This is total amount of memory on the machine
- **memory_avail** This is amount of available memory on the machine
- **cpus** This is number of detected CPUs on the machine.
- **uptime** This how long the machine has been running.
- **owner** This is user who sent the update.



The field **name** is intended to give a human-readable name to a service or
application which accepts incoming connections at **port**.


## ENVIRONMENT VARIABLES


- **CATALOG_HOST** Hostname of catalog server (same as **-c**).


## EXIT STATUS
On success, returns zero.  On failure, returns non-zero.

## EXAMPLES


The following example sends an update to the catalog server located at
**catalog.cse.nd.edu** with three custom fields.

```
% cat > test.json << EOF
{
    "type" : "node",
    "has_java" : true,
    "mode" : 3
}
EOF
% catalog_update -c catalog.cse.nd.edu -f test.json
```

## COPYRIGHT
The Cooperative Computing Tools are Copyright (C) 2022 The University of Notre Dame.  This software is distributed under the GNU General Public License.  See the file COPYING for details.

## SEE ALSO

- [Cooperative Computing Tools Documentation]("../index.html")
- [catalog_server(1)](catalog_server.md)  [catalog_update(1)](catalog_update.md)  [catalog_query(1)](catalog_query.md)  [chirp_status(1)](chirp_status.md)  [work_queue_status(1)](work_queue_status.md)   [deltadb_query(1)](deltadb_query.md)


CCTools
