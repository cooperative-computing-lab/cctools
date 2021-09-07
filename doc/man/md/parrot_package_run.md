






















# parrot_package_run(1)

## NAME
**parrot_package_run** - repeat a program within the package with the help of **parrot_run**

## SYNOPSIS
**parrot_package_run --package-path your-package-path [command]**

## DESCRIPTION
If **parrot_run** is used to repeat one experiment, one mountlist must be created so that the file access request of your program can be redirected into the package. **parrot_package_run** is used to create the mountlist and repeat your program within the package with the help of **parrot_run** and **mountlist**. If no command is given, a /bin/sh shell will be returned.

## OPTIONS

- **-p**,**--package-path**<br />The path of the package.
- **-e**,**--env-list**<br />The path of the environment file, each line is in the format of _&lt;key&gt;_=_&lt;value&gt;_. (Default: package-path/env_list)
- **-e**,**--env-list**<br />The path of the environment file, each line is in the format of _&lt;key&gt;_=_&lt;value&gt;_. (Default: package-path/env_list)
- **-h**,**--help**<br />Show this help message.


## EXIT STATUS
On success, returns zero. On failure, returns non-zero.

## EXAMPLES
To repeat one program within one package **/tmp/package** in a **bash** shell:
```
% parrot_package_run --package-path /tmp/package /bin/bash
```
After the execution of this command, one shell will be returned, where you can repeat your original program. After everything is done, exit **parrot_package_run**:
```
% exit
```
You can also directly set your command as the arguments of **parrot_package_run**. In this case, **parrot_package_run** will exit automatically after the command is finished, and you do not need to use **exit** to exit. However, your command must belong to the original command set executed inside **parrot_run** and preserved by **parrot_package_create**.
```
% parrot_package_run --package-path /tmp/package ls -al
```

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
% parrot_package_run --package-path /tmp/package /bin/bash
```
After the execution of this command, one shell will be returned, where you can repeat your original program. After everything is done, exit **parrot_package_run**:
```
% exit
```

## COPYRIGHT

The Cooperative Computing Tools are Copyright (C) 2005-2021 The University of Notre Dame.  This software is distributed under the GNU General Public License.  See the file COPYING for details.

## SEE ALSO


- [Cooperative Computing Tools Documentation]("../index.html")
- [Parrot User Manual]("../parrot.html")
- [parrot_run(1)](parrot_run.md) [parrot_cp(1)](parrot_cp.md) [parrot_getacl(1)](parrot_getacl.md)  [parrot_setacl(1)](parrot_setacl.md)  [parrot_mkalloc(1)](parrot_mkalloc.md)  [parrot_lsalloc(1)](parrot_lsalloc.md)  [parrot_locate(1)](parrot_locate.md)  [parrot_timeout(1)](parrot_timeout.md)  [parrot_whoami(1)](parrot_whoami.md)  [parrot_mount(1)](parrot_mount.md)  [parrot_md5(1)](parrot_md5.md)  [parrot_package_create(1)](parrot_package_create.md)  [parrot_package_run(1)](parrot_package_run.md)  [chroot_package_run(1)](chroot_package_run.md)


CCTools 7.3.2 FINAL
