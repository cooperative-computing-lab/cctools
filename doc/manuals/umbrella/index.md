# Umbrella User's Manual

## Overview

Umbrella is a tool for specifying and materializing comprehensive execution
environments, from the hardware all the way up to software and data. A user
simply invokes Umbrella with the desired task, and Umbrella parses the
specification, determines the minimum mechanism necessary to run the task,
downloads missing dependencies, and executes the application through the
available minimal mechanism, which may be direct execution, a system container
(Docker, chroot, Parrot), a local virtual machine (i.e., VMware), or
submission to a cloud environment (i.e., Amazon EC2) or grid environment
(i.e., Condor). The following figure shows the overview of Umbrella.

![](images/Umbrella.png)

The architecture of Umbrella is shown in the figure below.

![](images/umbrella-specification-big-picture.png)

An architecture can be divided in five parts:

1. User inputs. Consist of the task specification, input files of the task, and the output
directory.

- Umbrella tool. Instantiates the user's execution environment specification
  with the underlying execution engines.

- Execution engines. Execute the task.

- Remote archive. Stores the OS images, and software and data dependencies.

- Metadata database.  Maps the relationship between the dependency name
  referred in the specification and the actual storage location within the
  remote archive.

Currently, Umbrella supports the following execution engines:


* docker

* parrot

* local execution (Use docker if available, if not, use parrot.)

* destructive (Requires root as fulfils dependencies using rpms.)

* EC2

* HTcondor

!!! warning
    The Docker execution engine of Umbrella can not work together with AFS.  If
    you try to use the Docker execution engine of Umbrella, please do not
    install CCTools on your AFS directory.



## Getting Started

To get started using Umbrella, please begin by [installing CCTools](http://ccl.cse.nd.edu/software/manuals/install.html) on your system.

When your installation is ready, try the following example which uses a
Ray-Tracing application to illustrate how to execute an application with
Umbrella:

[povray.umbrella](http://ccl.cse.nd.edu/software/umbrella/database/povray/povray.umbrella)
```json
{
  "comment": "A ray-tracing application which creates video frames.",
  "hardware": {
    "cores": "2",
    "disk": "3GB",
    "arch": "x86_64",
    "memory": "2GB"
  },
  "kernel": {
    "version": ">=2.6.18",
    "name": "linux"
  },
  "os": {
    "name": "Redhat",
    "format": "tgz",
    "checksum": "62aa9bc37afe3f738052da5545832c80",
    "source": [
      "http://ccl.cse.nd.edu/research/data/hep-case-\nstudy/62aa9bc37afe3f738052da5545832c80/redhat-5.10-x86_64.tar.gz"
    ],
    "version": "5.10",
    "uncompressed_size": "1622159360",
    "id": "62aa9bc37afe3f738052da5545832c80",
    "size": "503195460"
  },
  "software": {
    "povray-3.6.1-redhat5-x86_64": {
      "format": "tgz",
      "checksum": "9b7f2362e6b927c8ef08c3f92599e47c",
      "source": [
        "http://ccl.cse.nd.edu/research/data/hep-case-\nstudy/9b7f2362e6b927c8ef08c3f92599e47c/povray-3.6.1-redhat5-x86_64.tar.gz"
      ],
      "action": "unpack",
      "mountpoint": "/software/povray-3.6.1-redhat5-x86_64",
      "mount_env": "POVRAY_PATH",
      "uncompressed_size": "3004423",
      "id": "9b7f2362e6b927c8ef08c3f92599e47c",
      "size": "1471457"
    }
  },
  "data": {
    "4_cubes.pov": {
      "format": "plain",
      "checksum": "c65266cd2b672854b821ed93028a877a",
      "source": [
        "http://ccl.cse.nd.edu/research/data/hep-case-\nstudy/c65266cd2b672854b821ed93028a877a/4_cubes.pov"
      ],
      "mountpoint": "/tmp/4_cubes.pov",
      "id": "c65266cd2b672854b821ed93028a877a",
      "size": "1757"
    },
    "WRC_RubiksCube.inc": {
      "format": "plain",
      "checksum": "2f8afdd09fc3a6177c6f1977bb3bdae7",
      "source": [
        "http://ccl.cse.nd.edu/research/data/hep-case-\nstudy/2f8afdd09fc3a6177c6f1977bb3bdae7/WRC_RubiksCube.inc"
      ],
      "mountpoint": "/tmp/WRC_RubiksCube.inc",
      "id": "2f8afdd09fc3a6177c6f1977bb3bdae7",
      "size": "28499"
    }
  },
  "environ": {
    "PWD": "/tmp"
  },
  "cmd": "povray +I/tmp/4_cubes.pov\n+O/tmp/frame000.png +K.0 -H50 -W50",
  "output": {
    "files": [
      "/tmp/frame000.png"
    ],
    "dirs": []
  }
}
```

To execute this task specification using the parrot engine:

```sh
$ umbrella --spec povray.umbrella --localdir /tmp/umbrella --output "/tmp/frame000.png=/tmp/umbrella/cubes.png"--sandbox_mode parrot --log umbrella.log run
```

After this umbrella command is finished, you can find the output file,
`/tmp/umbrella/cubes.png`. The output png file should look like:

![](images/umbrella-povray.png)

You can check the help document of umbrella for the option settings by running
the command:

```sh
$ umbrella -h
```


## Try CMS Applications with Umbrella

The part uses a CMS application as an example to illustrate how to execute a
CMS application with Umbrella.

(cms_complex.umbrella)[http://ccl.cse.nd.edu/software/umbrella/database/cms_complex/cms_complex.umbrella]
(cms_complex.sh)[http://ccl.cse.nd.edu/software/umbrella/database/cms_complex/cms_complex.sh]
```json
{
    "comment": "A ray-tracing application which creates video frames.",
    "hardware": {
        "cores": "2",
        "disk": "3GB",
        "arch": "x86_64",
        "memory": "2GB"
    },
    "kernel": {
        "version": ">=2.6.18",
        "name": "linux"
    },
    "os": {
        "name": "Redhat",
        "format": "tgz",
        "checksum": "62aa9bc37afe3f738052da5545832c80",
        "source": [
            "http://ccl.cse.nd.edu/research/data/hep-case-study/62aa9bc37afe3f738052da5545832c80/redhat-5.10-x86_64.tar.gz"
        ],
        "version": "5.10",
        "uncompressed_size": "1622159360",
        "id": "62aa9bc37afe3f738052da5545832c80",
        "size": "503195460"
    },
    "software": {
        "povray-3.6.1-redhat5-x86_64": {
            "format": "tgz",
            "checksum": "9b7f2362e6b927c8ef08c3f92599e47c",
            "source": [
                "http://ccl.cse.nd.edu/research/data/hep-case-study/9b7f2362e6b927c8ef08c3f92599e47c/povray-3.6.1-redhat5-x86_64.tar.gz"
            ],
            "action": "unpack",
            "mountpoint": "/software/povray-3.6.1-redhat5-x86_64",
            "mount_env": "POVRAY_PATH",
            "uncompressed_size": "3004423",
            "id": "9b7f2362e6b927c8ef08c3f92599e47c",
            "size": "1471457"
        }
    },
    "data": {
        "4_cubes.pov": {
            "format": "plain",
            "checksum": "c65266cd2b672854b821ed93028a877a",
            "source": [
                "http://ccl.cse.nd.edu/research/data/hep-case-study/c65266cd2b672854b821ed93028a877a/4_cubes.pov"
            ],
            "mountpoint": "/tmp/4_cubes.pov",
            "id": "c65266cd2b672854b821ed93028a877a",
            "size": "1757"
        },
        "WRC_RubiksCube.inc": {
            "format": "plain",
            "checksum": "2f8afdd09fc3a6177c6f1977bb3bdae7",
            "source": [
                "http://ccl.cse.nd.edu/research/data/hep-case-study/2f8afdd09fc3a6177c6f1977bb3bdae7/WRC_RubiksCube.inc"
            ],
            "mountpoint": "/tmp/WRC_RubiksCube.inc",
            "id": "2f8afdd09fc3a6177c6f1977bb3bdae7",
            "size": "28499"
        }
    },
    "environ": {
        "PWD": "/tmp"
    },
    "cmd": "povray +I/tmp/4_cubes.pov +O/tmp/frame000.png +K.0  -H50 -W50",
    "output": {
        "files": [
            "/tmp/frame000.png"
        ],
        "dirs": [
        ]
    }
}
```

The analysis code will create a directory called
`sim_job` and put the CMSSW software dependencies under it. Using parrot as the
engine when cvmfs *is* available in the machine:

```sh
$ umbrella  --sandbox_mode parrot  --log umbrella.log  --spec cms_complex.umbrella  --localdir /tmp/umbrella_test/  --output "/tmp/sim_job=/tmp/umbrella_test/parrot_cms_complex_output" --use_local_cvmfs  run`

Using parrot as the engine when cvmfs *is not* available in the machine:

```sh
$ umbrella  --sandbox_mode parrot  --log umbrella.log  --spec cms_complex.umbrella  --localdir /tmp/umbrella_test/  --output "/tmp/sim_job=/tmp/umbrella_test/parrot_cms_complex_output" --cvmfs_http_proxy http://cache01.hep.wisc.edu:3128  run`
```

After umbrella finishes executing the CMS application, you should see something like this:

```sh
21-May-2015 11:05:45 EDT  Initiating request to open LHE file file:/tmp/final_events_2381.lhe
21-May-2015 11:05:45 EDT  Successfully opened LHE file file:/tmp/final_events_2381.lhe
Begin processing the 1st record. Run 1, Event 1, LumiSection 1 at 21-May-2015 11:05:45.202 EDT
Begin processing the 2nd record. Run 1, Event 2, LumiSection 1 at 21-May-2015 11:05:45.204 EDT
Begin processing the 3rd record. Run 1, Event 3, LumiSection 1 at 21-May-2015 11:05:45.210 EDT
Begin processing the 4th record. Run 1, Event 4, LumiSection 1 at 21-May-2015 11:05:45.214 EDT
Begin processing the 5th record. Run 1, Event 5, LumiSection 1 at 21-May-2015 11:05:45.216 EDT
Begin processing the 6th record. Run 1, Event 6, LumiSection 1 at 21-May-2015 11:05:45.235 EDT
Begin processing the 7th record. Run 1, Event 7, LumiSection 1 at 21-May-2015 11:05:45.244 EDT
Begin processing the 8th record. Run 1, Event 8, LumiSection 1 at 21-May-2015 11:05:45.248 EDT
Begin processing the 9th record. Run 1, Event 9, LumiSection 1 at 21-May-2015 11:05:45.249 EDT
Begin processing the 10th record. Run 1, Event 10, LumiSection 1 at 21-May-2015 11:05:45.264 EDT

=============================================

MessageLogger Summary

 type     category        sev    module        subroutine        count    total
  ---- -------------------- -- ---------------- ----------------  -----    -----
      1 fileAction           -s AfterSource                            2        2

       type    category    Examples: run/evt        run/evt          run/evt
        ---- -------------------- ---------------- ---------------- ----------------
            1 fileAction           BeforeEvents     BeforeEvents

            Severity    # Occurrences   Total Occurrences
            --------    -------------   -----------------
            System                  2                   2
```


## Create Your Own Specifications

The Umbrella specification for an application is encoded in a JSON object. It includes the following sections:

### hardware (required)

  * **arch** (required): the hardware architecture the application needs to run on. Options: x86_64, i386, i686. Current support options: x86_64. Not case sensitive.
  * **cores** (optional): the number of cores the applications needs. Options: 1, 2, 3, 4, ... Default: 1.
  * **memory** (optional): the memory space the application needs in the unit of GB. Options: 1GB, 2GB, .... Default: 1GB. Not case sensitive.
  * **disk** (optional): the disk space the application needs in the unit of GB. Options: 1GB, 2GB, .... Default: 1GB. Not case sensitive.

### kernel (required):

  * **name** (required): the kernel type the application requires. Options: linux, windows. Current support options: linux. Not case sensitive.
  * **version** (required): the kernel version in the format of ` A.B.C` (A: kernel version; B: major revision of the kernel; C: the minor revision of the kernel). You can specify this attribute to a single value like `2.6.18` or a range like `>=2.6.18` or a range like `[2.6.18, 2.6.32]`.

### os (required)

  * **name** (required): the OS name. Options: redhat, centos, arch, .... Not case sensitive.
  * **version** (required): the OS version in the format of ` A.B`: A is the main version number and B is the minor version number. Exmaples: 5.10, 6.5. 
  * **id** (optional): the id of the OS image. There may exist multiple OS images for redhat 5.10, the id attribute uniquely identifies an OS image. 
  * **action** (required): the action on the downloaded dependencies. Options: ` none`, `unpack`. `none` leaves the downloaded dedendency as it is. `unpack` uncompresses the dependency. Default: unpack. Not case sensitive.

### software (optional)

Each software dependency has a case-sensitive name. The name of a software
dependency is in the format of 'A-B-C-D', where A is the software name, B is
the software version, C is OS distro name (the OS name followed by the main
version number, e.g., redhat5), D is hardware architecture.
`povray-3.6.1-redhat5-x86_64` is an example of this category.

  * **id** (optional): the id of the software. There may exist multiple versions of a software due to the difference of complication settings. the id attribute uniquely identifies a software. 
  * **action** (required): the action on the downloaded dependencies. Options: ` none`, `unpack`. `none` leaves the downloaded dedendency as it is. `unpack` uncompresses the dependency. Default: unpack. Not case sensitive.
  * **mode** (optional): the file mode set as a string, such as ` "0644"`. 
  * **mountpoint** (optional): the mountpoint of the software. Case sensitive.
  * **mount_env** (optional): Replace hard-coded paths in the shell script with environment variables to make it easy to redirect dependencies in the future. Case sensitive. In the above example, ` POVRAY_PATH` is used inside the user's source code instead of the path `/software/povray-3.6.1-redhat5-x86_64`.

!!! warning
    Even if both mountpoint and mount_env are optional, at least one of them must be provided.

#### Relationship between mountpoint and mount_env

**Case 1:** If only mountpoint is set to A in a specification, the dependency
will be downloaded into the umbrella local cache with the file path of D, and
a new mountpoint will be added into mount_dict (mount_dict[A] = D).

**Case 2:** If only mount_env is set to B in a specification, the dependency
will not be downloaded, package_search will be executed to get one remote
storage location, C, of the dependency, a new environment variable will be set
(env_para_dict[B] = C).

**Case 3:** If mountpoint is set to A and mount_env is set to B in a
specification, the dependency will be downloaded into the umbrella local cache
with the file path of D, and a new mountpoint will be added into mount_dict
(mount_dict[A] = D) and a new environment variable will also be set
(env_para_dict[B] = A).

### data (optional)

Each data dependency has a name. There is no special limitation on the name of
a data dependency. `WRC_RubiksCube.inc` is an example of this category.

  * **id** (optional): the id of the data. There may exist multiple versions of a data. the id attribute uniquely identifies a data. 
  * **action** (required): the action on the downloaded dependencies. Options: ` none`, `unpack`. `none` leaves the downloaded dedendency as it is. `unpack` uncompresses the dependency. Default: unpack. Not case sensitive.
  * **mode** (optional): the file mode set as a string, such as ` "0644"`. 
  * **mountpoint** (optional): the mountpoint of the data dependency. The same as the mountpoint attribute of the software section. Case sensitive.
  * **mount_env** (optional): the same as the mount_env attribute of the software section. Case sensitive.

!!! note
    Even if both mountpoint and mount_env are optional, at least one of them must be provided.

### environ (optional)

A list of key-value pairs. For example, `"HOME": "/home/hmeng"`, which sets
the HOME environment variable used by the sandbox to execute the applicition.
Case sensitive.

### cmd (optional)

The command to execute in the format of a string.

### output (optional)

This section allows the creator of an Umbrella spec to specify the output
files and directories. Correspondingly, there are two subsections: `files` (a
list of strings representing the output files) and `dirs` (a list of strings
representing the output dirs).

## Common Attributes of OS, Software, Data Dependencies

Each OS, software and data dependency can have the following attributes:
source, checksum, size, format, uncompressed_size, and upload.

These attributes are required in a self-contained umbrella specification, but
not required in an umbrella specification which is attached to a Metadata
Database.

  * **source** : a list of storage location of the dependency, which includes one or more resource URL. Currently Umbrella supports the URLs identifying the resources from the following remote services. For private resources from OSF, and S3, the corresponding authentication information can be provided through the Umbrella options.

|remote service|example path|
|-|-|
Local| /home/hmeng/data/input |
HTTP | http://www.somewhere.com/index.html |
HTTPS| https://ccl.cse.nd.edu/index.html |
Amazon S3| s3+https://s3.amazonaws.com/testhmeng/4_cubes.pov |
[Open Science Framework](https://osf.io/)| osf+https://files.osf.io/v1/...7559c3a |
Git | git+https://github.com/ayrodrig/OutreachExercise2010.git |
[CVMFS](http://cernvm.cern.ch/portal/filesystem)| cvmfs://cvmfs/cms.cern.ch

  * **checksum** : the checksum of the dependencies. Currently Umbrella only supports md5 checksum. Not case sensitive.
  * **format** : the perservation format of the dependency. Currently Umbrella supports two formats: `tgz` (gzip compressed tarball) and `plain` (plain text).
  * **size** : the size of the dependency in bytes.
  * **uncompressed_size** : the uncompressed size of the dependency in bytes, only meaningful when the format attribute is not plain text.
  * **upload (optional)** : whether this dependency will be uploaded into the storage services like OSF and S3 or not. The type of this field is boolean. This field can be set to be `false` or `true`.

#### Relationship of id and checksum and source

The checksum of each package can be used as the id of the package. However, it
is not necessary for them to be identical to each other. You can implement your
own mechanism once the id of each package is unique for one dependency. In case
when the checksum of one dependency is not provided or feasible, the first item
of the source section can be used as the value of the id attribute. For
example, in the CMS example, ` cmssw-4.2.8-slc5-amd64` is delivered through
cvmfs during runtime, and no checksum is provided, the url from the source
section, `cvmfs://cvmfs/cms.cern.ch` is used as the id of this package.


## Behaviors of Umbrella

### Execute your Application through Umbrella

For the following examples,
[cms_opendata_S.umbrella](http://ccl.cse.nd.edu/research/data/hep-case-
study/cms_opendata/cms_opendata_S.umbrella) is a self-contained umbrella
specification,
[cms_opendata.umbrella](http://ccl.cse.nd.edu/research/data/hep-case-
study/cms_opendata/cms_opendata.umbrella) is an umbrella specification which
does not include any metadata information.

The metadata information here includes: source, checksum, size, format, and
uncompressed_size.

#### Running an application from an umbrella specification

```sh
$ umbrella  --spec cms_opendata_S.umbrella  --localdir /tmp/umbrella_test/ --output "/tmp/sim_job=/tmp/umbrella_test/parrot_cms_opendata_output" --sandbox_mode parrot  --log umbrella.log  --cvmfs_http_proxy http://cache01.hep.wisc.edu:3128  run
```

For further information on the `run` command:

```sh
$ umbrella run help
```

`cms_opendata_S.umbrella` is self-contained, so no metadata database is needed.

!!! note
    This example uses parrot execution engine, for other execution engines, please check the Different Execution Engines of Umbrella section.

!!! note
    The cvmfs_http_proxy option is used to specify a http proxy for cvmfs access.  If your application does not need to access cvmfs, ignore this option.



#### Validating an Umbrella Spec File

##### Validate a self-contained Umbrella specificiation

```sh
$ umbrella --spec cms_opendata_S.umbrella validate
```


##### Validating an Umbrella specificiation with the help of a metadata db

```sh
$ umbrella  --spec cms_opendata.umbrella  --meta http://ccl.cse.nd.edu/software/umbrella/database/packages.json  validate
```

For further information on the `validate` command:

```sh
$ umbrella validate help
```



### Splitting an Umbrella Spec File into Spec and Meta

```sh
$ umbrella  --spec cms_opendata_S.umbrella  --log umbrella.log  split f2 db2
```

After the command is done, f2 becomes an umbrella specification without any
metadata information, db2 only includes the metadata information of the
dependencies.

For further information on the `validate` command:

```sh
$ umbrella split help
```

##### Expand an Umbrella Spec File into a Self-Contained Spec


```json
$ umbrella  --spec cms_opendata.umbrella  --meta http://ccl.cse.nd.edu/software/umbrella/database/packages.json --sandbox_mode parrot  --log umbrella.log  expand f1
```

After this command, f1 becomes self-contained including both the specification
and metadata info of its dependencies. The metadata info are abstract from the
metadata db provided through the --meta option.

For further information on the `expand` command:

```sh
$ umbrella expand help
```


##### Filter the Meta Info for an Umbrella Spec File from a Metadata DB


```json
$ umbrella  --spec cms_opendata.umbrella  --meta http://ccl.cse.nd.edu/software/umbrella/database/packages.json  --sandbox_mode parrot  --log umbrella.log  filter db1
```

After this command, db1 only includes the metadata information of the
dependencies involved in cms_opendata.umbrella.

For further information on the `expand` command:

```sh
$ umbrella filter help
```


##### Upload the Dependencies in an Umbrella Spec into the Amazon S3

```json
$ umbrella  --spec cms_complex_S.umbrella  --localdir /tmp/umbrella_test/  --log umbrella.log  upload s3 test_bucket public s3.umbrella
```

**test_bucket** is the Amazon S3 bucket name which will be created to hold the
dependencies. **public** specifies the access permission of the created bucket
and the new objects in it is public (Anyone with the s3 link can download the
resource). **s3.umbrella** is the new umbrella spec which has all the
dependencies from the Amazon S3.

After this command, a new file called **s3.umbrella** will be created and all
the dependencies in it are from the Amazon S3.


##### Upload the Dependencies in an Umbrella Spec into OSF

```json
$ umbrella  --spec cms_complex_S.umbrella  --localdir /tmp/umbrella_test/  --log umbrella.log  upload osf proj_cms public osf.umbrella`
```

**proj_cms** is the OSF project name which will be created to hold the
dependencies. **public** specifies the access permission of the created
project and the new objects in it is public (Anyone with the osf link can
download the resource). **osf.umbrella** is the new umbrella spec which has
all the dependencies from OSF.

After this command, a new file called **osf.umbrella** will be created and all
the dependencies in it are from OSF.

For further information on the `upload` command:

```sh
$ umbrella upload help
```


## Different Execution Engines of Umbrella

At runtime, Umbrella evaluates the local execution environment to see if it is
compatible with the specification. Umbrella evaluates the hardware resources
available, the kernel and OS distribution, and the software and data
dependencies. It then selects the mechanism necessary to deliver the desired
environment. In the case where Umbrella can not construct the desired
environment on the local machine, the user will be notified.

**Local Cache and Mounting Mechanism.** One cache directory will be set on
each execution node involved in the execution engine to avoid download the
same data from the remote archive repeatedly. Umbrella downloads and caches OS
images, software dependencies, and data dependencies in the host machine, and
then creates a sandbox to execute the application. To enable software
reusability by multiple users, Umbrella constructs the sandbox for each
application through mounting-based sandbox techniques.

The following figure shows the relationship between the remote archive, the
local cache and the sandbox for each application. `Sandbox 1` uses the root
filesystem of the host machine as the root filesystem and mounts the needed
software and data dependencies (`A` and `G`) into it. `Sandbox 2` needs to
construct a separate root filesystem which groups together the needed OS image
(`C`), software dependency (`A`).

![](images/umbrella-archive-sandbox-cache.png)

The following parts uses a Ray-Tracing application as an example to illustrate
how to use different execution engines of Umbrella to execute the application.

The specification for the application is [povray_S.umbrella](http://ccl.cse.nd.edu/software/umbrella/database/povray/povray_S.umbrella). It needs the two input files [4_cubes.pov](http://ccl.cse.nd.edu/software/umbrella/database/povray/4_cubes.pov)
and
[WRC_RubiksCube.inc](http://ccl.cse.nd.edu/software/umbrella/database/povray/WRC_RubiksCube.inc).

The command for this application is:`povray +I4_cubes.pov +Oframe000.png +K.0
-H50 -W50` Suppose you do your umbrella test under `/tmp/umbrella`. First
download the specification into `/tmp/umbrella`.


### Execute your Application through Umbrella - Parrot

```json
$ umbrella  --spec povray_S.umbrella  --localdir /tmp/umbrella_test/  --output "/tmp/frame000.png=/tmp/umbrella_test/parrot_povray"  --sandbox_mode parrot  --log umbrella.log  run`
```

### Execute your Application through Umbrella - Docker

```json
$ umbrella  --spec povray_S.umbrella  --localdir /tmp/umbrella_test/  --output "/tmp/frame000.png=/tmp/umbrella_test/docker_povray"  --sandbox_mode docker  --log umbrella.log  run`
```

!!! warning
    The Docker execution engine of Umbrella can not work together with AFS.  If
    you try to use the Docker execution engine of Umbrella, please do not
    install CCTools on your AFS directory.


### Execute your Application through Umbrella - EC2

```json
$ umbrella  --spec povray_S.umbrella  --localdir /tmp/umbrella_test/ --output "/tmp/frame000.png=/tmp/umbrella_test/ec2_povray/output.png" --sandbox_mode ec2  --ec2_log umbrella.log.ec2  --ec2_sshkey ~/bin/feb272015.pem  --ec2_key feb272015  --ec2_instance_type m3.medium --log umbrella.log  run
```

Using the `ec2 execution engine requires the composer of an Umbrella
specification to specify the AMIs, its region and default user in the os
section of the umbrella spec. For example,

```json
"ec2": { "ami": "ami-2cf8901c", "region": "us-west-2", "user": "ec2-user" }
```

To repeat an experiment using Umbrella ec2 execution engine, you need to
provide your EC2 key pair, security group, ssh key and instance type through
the umbrella options.

Note that the EC2 AMIs, the security groups, key pairs are all [regional
resources](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/resources.html). Therefore, please check the region setting of the desired AMI in the
umbrella spec file, and provide security group, key pairs, ssh keys in the
same region. The security group used should allow incoming ssh traffic.


## Create Your Own Metadata Database

When your have multiple relevant applications which share lots of their
dependencies, instead of putting metadata information of each dependency into
the Umbrella specification, you can ceate a metadata database which includes
all the metadata information.

Example metadata db:

[packages.json](http://ccl.cse.nd.edu/software/umbrella/database/packages.json)

```json
{
	"povray-3.6.1-redhat5-x86_64": {
		"9b7f2362e6b927c8ef08c3f92599e47c": {
			"source": [
				"http://ccl.cse.nd.edu/research/data/hep-case-study/9b7f2362e6b927c8ef08c3f92599e47c/povray-3.6.1-redhat5-x86_64.tar.gz"
			],
			"format": "tgz",
			"checksum": "9b7f2362e6b927c8ef08c3f92599e47c",
			"uncompressed_size": "3004423",
			"size": "1471457"
		}
	},
	"povray-3.6.1-redhat6-x86_64": {
		"b02ba86dd3081a703b4b01dc463e0499": {
			"source": [
				"http://ccl.cse.nd.edu/research/data/hep-case-study/b02ba86dd3081a703b4b01dc463e0499/povray-3.6.1-redhat6-x86_64.tar.gz"
			],
			"format": "tgz",
			"checksum": "b02ba86dd3081a703b4b01dc463e0499",
			"uncompressed_size": "3010560",
			"size": "1471452"
		}
	},
	"povray-3.6.1-centos6-x86_64": {
		"6fd5c05379d1bf2ff3f06dde024649ca": {
			"source": [
				"http://ccl.cse.nd.edu/research/data/hep-case-study/6fd5c05379d1bf2ff3f06dde024649ca/povray-3.6.1-centos6-x86_64.tar.gz"
			],
			"format": "tgz",
			"checksum": "6fd5c05379d1bf2ff3f06dde024649ca",
			"uncompressed_size": "3010560",
			"size": "1471451"
		}
	},
	"centos-6.6-x86_64": {
		"902703f016e0f930a870eaf9cb31640b": {
			"source": [
				"http://ccl.cse.nd.edu/research/data/hep-case-study/902703f016e0f930a870eaf9cb31640b/centos-6.6-x86_64.tar.gz"
			],
			"format": "tgz",
			"checksum": "902703f016e0f930a870eaf9cb31640b",
			"uncompressed_size": "212684800",
			"size": "72213624"
		}
	},
	"cms_siteconf_local_cvmfs": {
		"2efd5cbb3424fe6b4a74294c84d0fb43": {
			"source": [
				"http://ccl.cse.nd.edu/research/data/hep-case-study/2efd5cbb3424fe6b4a74294c84d0fb43/SITECONF.tar.gz"
			],
			"format": "tgz",
			"checksum": "2efd5cbb3424fe6b4a74294c84d0fb43",
			"uncompressed_size": "11619",
			"size": "935"
		}
	},
	"cmssw-4.2.8-slc5-amd64": {
		"cvmfs://cvmfs/cms.cern.ch": {
			"source": [
				"cvmfs://cvmfs/cms.cern.ch"
			]
		}
	},
	"cmssw-5.3.11-slc5-amd64": {
		"cvmfs://cvmfs/cms.cern.ch": {
			"source": [
				"cvmfs://cvmfs/cms.cern.ch"
			]
		}
	},
	"cmssw-5.2.5-slc5-amd64": {
		"cvmfs://cvmfs/cms.cern.ch": {
			"source": [
				"cvmfs://cvmfs/cms.cern.ch"
			]
		}
	},
	"redhat-5.10-x86_64": {
		"62aa9bc37afe3f738052da5545832c80": {
			"source": [
				"http://ccl.cse.nd.edu/research/data/hep-case-study/62aa9bc37afe3f738052da5545832c80/redhat-5.10-x86_64.tar.gz"
			],
			"format": "tgz",
			"checksum": "62aa9bc37afe3f738052da5545832c80",
			"uncompressed_size": "1622159360",
			"size": "503195460"
		}
	},
	"redhat-6.5-x86_64": {
		"669ab5ef94af84d273f8f92a86b7907a": {
			"source": [
				"http://ccl.cse.nd.edu/research/data/hep-case-study/669ab5ef94af84d273f8f92a86b7907a/redhat-6.5-x86_64.tar.gz"
			],
			"format": "tgz",
			"checksum": "669ab5ef94af84d273f8f92a86b7907a",
			"uncompressed_size": "1743656960",
			"size": "633848940"
		}
	},
	"python-2.6.9-redhat5-x86_64": {
		"c9da9e46b3ce0f7f9885ce60077c45c5": {
			"source": [
				"http://ccl.cse.nd.edu/research/data/hep-case-study/c9da9e46b3ce0f7f9885ce60077c45c5/python-2.6.9-redhat5-x86_64.tar.gz"
			],
			"format": "tgz",
			"checksum": "c9da9e46b3ce0f7f9885ce60077c45c5",
			"uncompressed_size": "81591992",
			"size": "23037031"
		}
	},
	"python-2.6.9-redhat6-x86_64": {
		"df37d1ae059e218f319b28029cbdaffc": {
			"source": [
				"http://ccl.cse.nd.edu/research/data/hep-case-study/df36d1ae059e218f319b28029cbdaffc/python-2.6.9-redhat6-x86_64.tar.gz"
			],
			"format": "tgz",
			"checksum": "df36d1ae059e218f319b28029cbdaffc",
			"uncompressed_size": "84099190",
			"size": "22936670"
		}
	},
	"git-1.7.9-redhat5-x86_64": {
		"e20b34ee6e39d84cf65c5f8edae8a4a1": {
			"source": [
				"http://ccl.cse.nd.edu/research/data/hep-case-study/e20b34ee6e39d84cf65c5f8edae8a4a1/git-1.7.9-redhat5-x86_64.tar.gz"
			],
			"format": "tgz",
			"checksum": "e20b34ee6e39d84cf65c5f8edae8a4a1",
			"uncompressed_size": "46436031",
			"size": "16930687"
		}
	},
	"demoanalyzer_cfg.py": {
		"b0d3eb7874304ab2f75129646a311b12": {
			"source": [
				"http://ccl.cse.nd.edu/research/data/hep-case-study/b0d3eb7874304ab2f75129646a311b12/demoanalyzer_cfg.py"
			],
			"format": "plain",
			"checksum": "b0d3eb7874304ab2f75129646a311b12",
			"size": "623"
		}
	},
	"OutreachExercise2010.git": {
		"git+https://github.com/ayrodrig/OutreachExercise2010.git": {
			"source": [
				"git+https://github.com/ayrodrig/OutreachExercise2010.git"
			],
			"branch": "master"
		}
	},
	"cms_opendata_git": {
		"https://github.com/ayrodrig/OutreachExercise2010.git": {
			"source": [
				"https://github.com/ayrodrig/OutreachExercise2010.git"
			]
		}
	},
	"00459D48-EB70-E011-AF09-90E6BA19A252.root": {
		"69836a0b460adbb47f76032fe1c7e0aa": {
			"source": [
				"eospublic.cern.ch//eos/opendata/cms/Run2010B/Mu/AOD/Apr21ReReco-v1/0000/00459D48-EB70-E011-AF09-90E6BA19A252.root"
			]
		}
	},
	"cms_opendata.sh": {
		"2f2051dff6ca75ff67e71f95e9271857": {
			"source": [
				"http://ccl.cse.nd.edu/research/data/hep-case-study/2f2051dff6ca75ff67e71f95e9271857/cms_opendata.sh"
			],
			"format": "plain",
			"checksum": "2f2051dff6ca75ff67e71f95e9271857",
			"size": "726"
		},
		"a6f9d99bcc08adb019ed6f3c31d9c090": {
			"source": [
				"http://ccl.cse.nd.edu/research/data/hep-case-study/a6f9d99bcc08adb019ed6f3c31d9c090/cms_opendata.sh"
			],
			"format": "plain",
			"checksum": "a6f9d99bcc08adb019ed6f3c31d9c090",
			"size": "1444"
		}
	},
	"cms_simple.sh": {
		"9d4b969f93743ded4a4830376b2038b9": {
			"source": [
				"http://ccl.cse.nd.edu/research/data/hep-case-study/9d4b969f93743ded4a4830376b2038b9/cms_simple.sh"
			],
			"format": "plain",
			"checksum": "9d4b969f93743ded4a4830376b2038b9",
			"size": "348"
		}
	},
	"cms_complex.sh": {
		"9f8587e9ef90ab4f5de8b3c9ab5cf0cb": {
			"source": [
				"http://ccl.cse.nd.edu/research/data/hep-case-study/9f8587e9ef90ab4f5de8b3c9ab5cf0cb/cms_complex.sh"
			],
			"format": "plain",
			"checksum": "9f8587e9ef90ab4f5de8b3c9ab5cf0cb",
			"size": "399"
		}
	},	
	"git_protocol.sh": {
		"518876dbe4dca81bd19e823b6ea1f866": {
			"source": [
				"http://ccl.cse.nd.edu/research/data/hep-case-study/518876dbe4dca81bd19e823b6ea1f866/git_protocol.sh"
			],
			"format": "plain",
			"checksum": "518876dbe4dca81bd19e823b6ea1f866",
			"size": "546"
		}
	},
	"4_cubes.pov": {
		"c65266cd2b672854b821ed93028a877a": {
			"source": [
				"http://ccl.cse.nd.edu/research/data/hep-case-study/c65266cd2b672854b821ed93028a877a/4_cubes.pov"
			],
			"format": "plain",
			"checksum": "c65266cd2b672854b821ed93028a877a",
			"size": "1757"
		}
	},
	"WRC_RubiksCube.inc": {
		"2f8afdd09fc3a6177c6f1977bb3bdae7": {
			"source": [
				"http://ccl.cse.nd.edu/research/data/hep-case-study/2f8afdd09fc3a6177c6f1977bb3bdae7/WRC_RubiksCube.inc"
			],
			"format": "plain",
			"checksum": "2f8afdd09fc3a6177c6f1977bb3bdae7",
			"size": "28499"
		}
	},
	"yum.conf": {
		"33144cbf8a3bd638ff84948f3e26f31a": {
			"source": [
				"http://ccl.cse.nd.edu/research/data/hep-case-study/33144cbf8a3bd638ff84948f3e26f31a/yum.conf"
			],
			"format": "plain",
			"checksum": "33144cbf8a3bd638ff84948f3e26f31a",
			"size": "984"
		}
	},
	"final_events_2381.lhe": {
		"cb9878132aad42e7db30eabd214be8e2": {
			"source": [
				"http://ccl.cse.nd.edu/research/data/hep-case-study/cb9878132aad42e7db30eabd214be8e2/final_events_2381.lhe"
			],
			"format": "plain",
			"checksum": "cb9878132aad42e7db30eabd214be8e2",
			"size": "17840176"
		}
	}
}
```

### Naming Rules of Dependencies

The name of a software dependency is in the format of 'A-B-C-D', where A is
the software name, B is the software version, C is OS distro name, D is
hardware architecture. `povray-3.6.1-redhat5-x86_64` is an example of this
category.

The name of an OS image dependency is in the format of 'A-B-C', where A is the
OS name, B is the OS version, C is hardware architecture. `redhat-5.10-x86_64`
is an example of this category.

There is no special limitation on the name of a data dependency.
`final_events_2381.lhe` is an example of this category.


#### Multiple Packages for One Dependency

According to the building and compilation settings, there may be multiple
packages for one dependency. In this case, all the packages for one dependency
will be organized together and the `id` attribute of each package will be used
as item key to differentiate different packages.

For example, for the software dependency `povray-3.6.1-redhat5-x86_64`, there
are two different packages: one with the id of
`9b7f2362e6b927c8ef08c3f92599e47c` and one with the id of `c9da9e46b3ce0f7f9885ce60077c45c5`.

Each package may include the following attributes: source, checksum, size,
format, and uncompressed_size.

#### Relationship of id and checksum and source

The checksum of each package
in the archive can be used as the id of the package in our implementation of
metadata database. However, it is not necessary for them to be identical to
each other. You can implement your metadata database in a different semantics
once the id of each package is unique for one dependency. In case when the
checksum of one dependency is not provided or feasible, the first item of the
source section will be used.

For example, in the above example, `cmssw-4.2.8-slc5-amd64` is delivered through cvmfs during runtime, and no
checksum is provided, the url from the source section,
`cvmfs://cvmfs/cms.cern.ch` is used as the id of this package.

#### Organization of the Local Cache

Within the local cache, the id of each package dependency will be used to
create a directory under the ` <localdir>/cache`, then the package will be put
into `<localdir>/cache/<idgt;/<name>`. Therefore,
`<localdir>/cache/9b7f2362e6b927c8ef08c3f92599e47c/povray-3.6.1-redhat5-x86_64.tar.gz`
will be the local location of the first povray package shown in the above
example. The uncompressed directory of the tarball will be
`<localdir>/cache/9b7f2362e6b927c8ef08c3f92599e47c/povray-3.6.1-redhat5-x86_64`.

#### Organization of the Remote Archive

Within the remote archive, to differentiate multiple packages for the same
dependency and the different dependencies, a directory with the name of the
checksum of each package will be created and functions as the direct parent
directory of the package.  Therefore, in the remote archive, there is a
directory named ` 9b7f2362e6b927c8ef08c3f92599e47c`, under which exists
`povray-3.6.1-redhat5-x86_64.tar.gz` However, the organization of the remote
archive can be in other format, once you have a way to differentiate the
packages.

If you want to customize your own metadata database, please follow the
requirements above and then tell umbrella through `--meta` option to use your
own metadata database. For Example:

```sh
$ umbrella  --spec povray.umbrella  --meta http://ccl.cse.nd.edu/software/umbrella/database/packages.json  --localdir /tmp/umbrella_test/  --output "/tmp/frame000.png=/tmp/umbrella_test/local_povray"  --sandbox_mode local --log umbrella.log  run
```

There are two ways to create an Umbrella metadata database:

1. Use `umbrella --spec spec_filename split new_spec_filename db_filename` to abstract all the
metadata information from a self-contained umbrella specification.

2. Follow the format of the metadata database and create your own metadata
database manually.


#### Umbrella Support for CMS Application

As for CMS applications which need software from CVMFS, Umbrella first checks
whether the execution node has CVMFS installed or not. If CVMFS is installed
and its CMS repository is mounted as ` /cvmfs/cms.cern.ch`, the application
can run directly without the help from sandboxing techniques. If CVMFS is not
installed or the mountpoint `/cvmfs/cms.cern.ch` is not found,
[Parrot](http://ccl.cse.nd.edu/software/parrot/) will be used to help access
software from CVMFS.

Like other software dependencies, the dependency of CMSSW will be specified
inside the `software` section of the specification, however, you only need to
specify the `mountpoint` attribute. The `id` and `action` attributes for the
CMSSW dependency will be ignored even if they are specified. In fact, the
support for CMS applications are hardcoded inside the source code of Umbrella,
not inside the metadata database.

If you want to run this CMS application through Umbrella, please check the Try
CMS Applications with Umbrella.


# Further Information

Umbrella is Copyright (C) 2015 The University of Notre Dame.  
All rights reserved.  
This software is distributed under the GNU General Public License.  
See the file COPYING for details.

**Please use the following citation for Umbrella in a scientific
publication:**

  * Haiyan Meng and Douglas Thain, [Umbrella: A Portable Environment Creator for Reproducible Computing on Clusters, Clouds, and Grids](http://ccl.cse.nd.edu/research/papers/umbrella-vtdc15.pdf), Workshop on Virtualization Technologies in Distributed Computing (VTDC) at HPDC, June, 2015. DOI: 10.1145/2755979.2755982


**Last edited: August 2019**

