






















# confuga_adm(1)

## NAME
**Confuga Administration** - Administrating the Confuga cluster.

## SYNOPSIS
**confuga_adm [options] _&lt;Confuga URI&gt;_ _&lt;command&gt;_ [command options] ...**
**confuga_adm [options] _&lt;Confuga URI&gt;_ _&lt;command&gt;_ [command options] ...**

## DESCRIPTION


Performs administrative commands on the Confuga cluster.


For complete details with examples, see the [Confuga User's Manual](http://ccl.cse.nd.edu/software/manuals/confuga.html).

## OPTIONS


- **-d**,**--debug=_&lt;flag&gt;_**<br />Enable debugging for this sybsystem
- **-h**,**--help**<br />Give help information.
- **-o**,**--debug-file=_&lt;file&gt;_**<br />Write debugging output to this file. By default, debugging is sent to stderr (":stderr"). You may specify logs to be sent to stdout (":stdout") instead.
- **-v**,**--version**<br />Show version info.


## COMMANDS


- **sn-add [-r _&lt;root&gt;_] [-p _&lt;password-file&gt;_] --uuid|--address _&lt;uuid|address&gt;_** Add a storage node to the cluster. Using the UUID of the Chirp server is recommended.
- **sn-add [-r _&lt;root&gt;_] [-p _&lt;password-file&gt;_] --uuid|address _&lt;uuid|address&gt;_** Add a storage node to the cluster. Using the UUID of the Chirp server is recommended.
- **sn-rm [options] --uuid|--address _&lt;uuid|address&gt;_** Remove a storage from the cluster. Using the UUID of the Chirp server is recommended. The storage node is removed when Confuga no longer relies on it to maintain minimum replication for files and when the storage node completes all running jobs.


## EXAMPLES


Add a storage node with Confuga state stored in **/u/joe/confuga**:

```
confuga_adm sn-add -r /u/joe/confuga address 10.0.1.125:9090
```


Remove a storage node:

```
confuga_adm sn-rm address 10.0.1.125:9090
```

## COPYRIGHT
The Cooperative Computing Tools are Copyright (C) 2005-2021 The University of Notre Dame.  This software is distributed under the GNU General Public License.  See the file COPYING for details.

## SEE ALSO
[confuga(1)](confuga.md)

CCTools
