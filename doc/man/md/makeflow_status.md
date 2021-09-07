






















# makeflow_status(1)

## NAME
**makeflow_status** - command line tool retrieving the status of makeflow programs.

## SYNOPSIS
****makeflow_status [options]****

## DESCRIPTION

**makeflow_status** retrieves the status of makeflow programs and prints out a report to **stdout**. By using flags, users can filter out certain responses, such as only finding reports of a certain projet, or a certain project owner.


## OPTIONS

- **-M --project <project>** The project on which to filter results.
- **-u --username <user>** The owner on which to filter results.
- **-s --server <server>** The catalog server to retrieve the reports from.
- **-p --port <port>** The port to contact the catalog server on.
- **-t --timeout <time>** Set remote operation timeout.
- **-h, --help** Show help text.


## ENVIRONMENT VARIABLES


- ****CATALOG_HOST**** The catalog server to retrieve reports from (same as **-s**).
- ****CATALOG_PORT**** The port to contact the catalog server on (same as **-p**).


## EXIT STATUS
On success, returns 0 and prints out the report to stdout.

## EXAMPLES

Retrieving reports related to project "awesome"

```
% makeflow_status -M awesome
```


## COPYRIGHT

The Cooperative Computing Tools are Copyright (C) 2005-2021 The University of Notre Dame.  This software is distributed under the GNU General Public License.  See the file COPYING for details.

CCTools 8.0.0 DEVELOPMENT
