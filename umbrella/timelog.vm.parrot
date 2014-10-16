[ec2-user@ip-172-31-9-52 ~]$ ./umbrella -T local -c povray_redhat5.json -i '4_cubes.pov, WRC_RubiksCube.inc' -s ~/umbrella_test/ -o ~/umbrella_test/parrot_povray_redhat5 run "povray +I4_cubes.pov +Oframe000.png +K.0  -H5000 -W5000"
['/home/ec2-user/4_cubes.pov', '/home/ec2-user/WRC_RubiksCube.inc']
/home/ec2-user/umbrella_test/1
Specification_process start: 2014-10-20 23:37:07.987673
Execution environment checking ...
The information of the host machine is:
Linux ip-172-31-9-52 2.6.32-431.17.1.el6.x86_64 #1 SMP Fri Apr 11 17:27:00 EDT 2014 x86_64 x86_64 
execution_environment_check end: 2014-10-20 23:37:07.994904
Installing software dependencies ...
dependency_process(povray) start: 2014-10-20 23:37:07.995007
Download software from https://www3.nd.edu/~ccl/research/data/hep-case-study/povray-x86_64-redhat5.tar.gz into the dir (/home/ec2-user/umbrella_test/cache/povray-x86_64-redhat5)
dependency_process(povray) end: 2014-10-20 23:37:10.056097
linux_disto: redhat5
host_linux_distro: redhat6
The required linux distro specified in the specification is redhat5; the linux distro of the host machine is redhat6. The redhat5 os images will be downloaded.
dependency_process(redhat) start: 2014-10-20 23:37:10.056191
Download software from https://www3.nd.edu/~ccl/research/data/hep-case-study/redhat-5.10-x86_64.tar.gz into the dir (/home/ec2-user/umbrella_test/cache/redhat-5.10-x86_64)
dependency_process(redhat) end: 2014-10-20 23:39:58.839233
dependency_process(cctools) start: 2014-10-20 23:39:58.839638
Download software from https://www3.nd.edu/~ccl/research/data/hep-case-study/cctools-x86_64-redhat6.tar.gz into the dir (/home/ec2-user/umbrella_test/cache/cctools-x86_64-redhat6)
dependency_process(cctools) end: 2014-10-20 23:40:04.333589
software_dependencies_installation end: 2014-10-20 23:40:04.333632
workflow_repeat
local-parrot pre-processing start: 2014-10-20 23:40:04.333713
uid: 500(ec2-user); gid: 500(ec2-user)
{u'/software/povray-x86_64-redhat5': u'/home/ec2-user/umbrella_test/cache/povray-x86_64-redhat5', '/': u'/home/ec2-user/umbrella_test/cache/redhat-5.10-x86_64'}
need to create rootfs
ec2-user is included in /etc/passwd!
ec2-user is included in /etc/group!
chroot_path_env 
{u'/software/povray-x86_64-redhat5': u'/home/ec2-user/umbrella_test/cache/povray-x86_64-redhat5'}

The environment variables:
{'LESSOPEN': '|/usr/bin/lesspipe.sh %s', 'SSH_CLIENT': '74.94.121.59 58370 22', 'CVS_RSH': 'ssh', 'LOGNAME': 'root', 'USER': 'ec2-user', 'INPUTRC': '/etc/inputrc', 'HOME': '/home/ec2-user/umbrella_test/1/ec2-user', 'PATH': u'/software/povray-x86_64-redhat5/bin:/usr/kerberos/sbin:/usr/kerberos/bin:/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin:/root/bin', 'LANG': 'en_US.UTF-8', 'TERM': 'xterm-color', 'SHELL': '/bin/bash', 'SHLVL': '1', 'G_BROKEN_FILENAMES': '1', 'HISTSIZE': '1000', 'PARROT_MOUNT_FILE': '/home/ec2-user/umbrella_test/1/mountlist', 'PARROT_LDSO_PATH': u'/home/ec2-user/umbrella_test/cache/redhat-5.10-x86_64/lib64/ld-linux-x86-64.so.2', '_': '/bin/env', 'SSH_CONNECTION': '74.94.121.59 58370 129.74.246.188 22', 'SSH_TTY': '/dev/pts/0', 'OLDPWD': '/root', 'HOSTNAME': 'ndcloudvm041', 'PWD': '/home/ec2-user/umbrella_test/1', 'MAIL': '/var/spool/mail/root', 'LS_COLORS': 'no=00:fi=00:di=01;34:ln=01;36:pi=40;33:so=01;35:bd=40;33;01:cd=40;33;01:or=01;05;37;41:mi=01;05;37;41:ex=01;32:*.cmd=01;32:*.exe=01;32:*.com=01;32:*.btm=01;32:*.bat=01;32:*.sh=01;32:*.csh=01;32:*.tar=01;31:*.tgz=01;31:*.arj=01;31:*.taz=01;31:*.lzh=01;31:*.zip=01;31:*.z=01;31:*.Z=01;31:*.gz=01;31:*.bz2=01;31:*.bz=01;31:*.tz=01;31:*.rpm=01;31:*.cpio=01;31:*.jpg=01;35:*.gif=01;35:*.bmp=01;35:*.xbm=01;35:*.xpm=01;35:*.png=01;35:*.tif=01;35:'}
local-parrot download successfulness testing start: 2014-10-20 23:40:04.335371

Here are some tests to illustrate the cctools and os images are downloaded********
du -hs /home/ec2-user/umbrella_test/cache/cctools-x86_64-redhat6
64M     /home/ec2-user/umbrella_test/cache/cctools-x86_64-redhat6

ls /home/ec2-user/umbrella_test/cache/redhat-5.10-x86_64
bin
boot
common-mountlist
env_list
etc
lib
lib64
mountlist
opt
root
sbin
special_files
tmp
usr
var

Here are some tests to illustrate the cctools and os images are downloaded********

local-parrot download successfulness testing end: 2014-10-20 23:40:04.707862
local-parrot pre-processing end: 2014-10-20 23:40:04.708032
which: no git in (/software/povray-x86_64-redhat5/bin:/usr/kerberos/sbin:/usr/kerberos/bin:/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/



Scene Statistics
  Finite objects:         6944
  Infinite objects:          1
  Light sources:             2
  Total:                  6947

  0:25:40 Rendering line 5000 of 5000
  0:25:42 Done Tracing
Render Statistics
Image Resolution 5000 x 5000
----------------------------------------------------------------------------
Pixels:         25000000   Samples:        25000000   Smpls/Pxl: 1.00
Rays:          240398200   Saved:                 0   Max Level: 2/5
----------------------------------------------------------------------------
Ray->Shape Intersection          Tests       Succeeded  Percentage
----------------------------------------------------------------------------
Box                         2936898146       309596088     10.54
Cone/Cylinder               1223434152       163795512     13.39
Plane                       1151957188        25000000      2.17
Sphere                        91976227        18542794     20.16
Bounding Box               34395965287      9312054137     27.07
Vista Buffer                1368617712       815901821     59.62
----------------------------------------------------------------------------
Calls to Noise:                   0   Calls to DNoise:       215398210
----------------------------------------------------------------------------
Shadow Ray Tests:        1822210870   Succeeded:             312821299
Reflected Rays:           215398200
----------------------------------------------------------------------------
Smallest Alloc:                   9 bytes
Largest  Alloc:              100028 bytes
Peak memory used:           6101712 bytes
Total Scene Processing Times
  Parse Time:    0 hours  0 minutes  2 seconds (2 seconds)
  Photon Time:   0 hours  0 minutes  0 seconds (0 seconds)
  Render Time:   0 hours 25 minutes 42 seconds (1542 seconds)
  Total Time:    0 hours 25 minutes 44 seconds (1544 seconds)
local-parrot user_cmd execution end: 2014-10-21 00:05:49.361350
Rename the sandbox dir(/home/ec2-user/umbrella_test/1) to the output directory(/home/ec2-user/umbrella_test/parrot_povray_redhat5)
local-parrot post processing end: 2014-10-21 00:05:49.361509
workflow_repeat end: 2014-10-21 00:05:49.361611

