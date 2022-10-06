






















# make_growfs(1)

## NAME
**make_growfs** - generate directory listings for the GROW filesystem

## SYNOPSIS
**make_growfs [options] _&lt;directory&gt;_**

## DESCRIPTION

**make_growfs** prepares a local filesystem to be exported as
a GROW filesystem which can be accessed by [parrot_run(1)](parrot_run.md).
Given a directory as an argument, it recursively visits all of
the directories underneath and creates files named **.__growfsdir**
that summarize the metadata of all files in that directory.

Once the directory files are generated, the files may be accessed
through a web server as if there were on a full-fledged filesystem
with complete metadata.

## OPTIONS


- **-v**<br />Give verbose messages.
- **-K**<br />Create checksums for files. (default)
- **-k**<br />Disable checksums for files.
- **-f**<br />Follow all symbolic links.
- **-F**<br />Do not follow any symbolic links.
- **-a**<br />Only follow links that fall outside the root.  (default)
- **-h**<br />Show help text.


## EXIT STATUS
On success, returns zero.  On failure, returns non-zero.

## EXAMPLES

Suppose that your university web server exports the
directory **/home/fred/www** as **http://www.somewhere.edu/fred**.
To create a GROW filesystem, put whatever files and directories you
like into **/home/fred/www**.  Then, run the following to generate
the GROW data:

```
% make_growfs /home/fred/www
```

Now that the GROW data is generated, you can use [parrot_run(1)](parrot_run.md)
to treat the web address as a read-only filesystem:

```
% parrot_run bash
% cd /growfs/www.somewhere.edu/fred
% ls -la
```

## COPYRIGHT

The Cooperative Computing Tools are Copyright (C) 2022 The University of Notre Dame.  This software is distributed under the GNU General Public License.  See the file COPYING for details.

## SEE ALSO


- [The Cooperative Computing Tools]("http://ccl.cse.nd.edu/software/manuals")
- [Parrot User Manual]("http://ccl.cse.nd.edu/software/manuals/parrot.html")
- [parrot_run(1)](parrot_run.md)


CCTools
