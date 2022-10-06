






















# parrot_package_create(1)

## NAME
**parrot_package_create** - generate a package based on the accessed files and the preserved environment variables

## SYNOPSIS
**parrot_package_create [options]**

## DESCRIPTION
After recording the accessed files and environment variables of one program with the help of the **--name-list** parameter and the **--env-list** of **parrot_run**, **parrot_package_create** can generate a package containing all the accessed files. You can also add the dependencies recorded in a new namelist file into an existing package.

## OPTIONS

- **-a**,**--add=_&lt;path&gt;_**<br />The path of an existing package.
- **-e**,**--env-list=_&lt;path&gt;_**<br />The path of the environment variables.
- **--new-env=_&lt;path&gt;_**<br />The relative path of the environment variable file under the package.
- **-n**,**--name-list=_&lt;path&gt;_**<br />The path of the namelist list.
- **-p**,**--package-path=_&lt;path&gt;_**<br />The path of the package.
- **-d**,**--debug=_&lt;flag&gt;_**<br />Enable debugging for this sub-system.
- **-o**,**--debug-file=_&lt;file&gt;_**<br />Write debugging output to this file. By default, debugging is sent to stderr (":stderr"). You may specify logs to be sent to stdout (":stdout") instead.
- **-h**,**--help**<br />Show the help info.


## EXIT STATUS
On success, returns zero. On failure, returns non-zero.

## EXAMPLES
To generate the package corresponding to **namelist** and **envlist**:
```
% parrot_package_create --name-list namelist --env-list envlist --package-path /tmp/package
```
After executing this command, one package with the path of **/tmp/package** will be generated.


Here is a short instruction about how to make use of **parrot_run**, **parrot_package_create** and **parrot_package_run**
to generate one package for your experiment and repeat your experiment within your package.

Step 1: Run your program under **parrot_run** and using **--name-list** and **--env-list** parameters to
record the filename list and environment variables.
```
% parrot_run --name-list namelist --env-list envlist /bin/bash
```
After the execution of this command, you can run your program inside **parrot_run**. At the end of step 1, one file named **namelist** containing all the accessed file names and one file named **envlist** containing environment variables will be generated.
After everything is done, exit **parrot_run**:
```
% exit
```

Step 2: Using **parrot_package_create** to generate a package.
```
% parrot_package_create --name-list namelist --env-path envlist --package-path /tmp/package
```
At the end of step 2, one package with the path of **/tmp/package** will be generated.

Step 3: Repeat your program within your package.
```
% parrot_package_run --package-path /tmp/package --shell-type bash ...
```
After the execution of this command, one shell will be returned, where you can repeat your original program (Please replace **--shell-type** parameter with the shell type you actually used). After everything is done, exit **parrot_package_run**:
```
% exit
```

You can also add the dependencies recorded in a new namelist file, **namelist1**, into an existing package:
```
% parrot_package_create --name-list namelist1 --env-list envlist1 --new-env envlist1  --add /tmp/package
```
After executing this command, all the new dependencies mentioned in **namelist1** will be added into **/tmp/package**, the new envlist, **envlist1**, will also be added into **/tmp/package** with the name specified by the **--new-env** option.

## COPYRIGHT

The Cooperative Computing Tools are Copyright (C) 2022 The University of Notre Dame.  This software is distributed under the GNU General Public License.  See the file COPYING for details.

## SEE ALSO

- [Cooperative Computing Tools Documentation]("../index.html")
- [Parrot User Manual]("../parrot.html")
- [parrot_run(1)](parrot_run.md) [parrot_cp(1)](parrot_cp.md) [parrot_getacl(1)](parrot_getacl.md)  [parrot_setacl(1)](parrot_setacl.md)  [parrot_mkalloc(1)](parrot_mkalloc.md)  [parrot_lsalloc(1)](parrot_lsalloc.md)  [parrot_locate(1)](parrot_locate.md)  [parrot_timeout(1)](parrot_timeout.md)  [parrot_whoami(1)](parrot_whoami.md)  [parrot_mount(1)](parrot_mount.md)  [parrot_md5(1)](parrot_md5.md)  [parrot_package_create(1)](parrot_package_create.md)  [parrot_package_run(1)](parrot_package_run.md)  [chroot_package_run(1)](chroot_package_run.md)

CCTools
