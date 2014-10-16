hmeng@hmeng ~/git-development/cctools/umbrella$ ./umbrella -T local -c povray_redhat5.json -i '4_cubes.pov, WRC_RubiksCube.inc' -s ~/umbrella_test/ -o ~/umbrella_test/parrot_povray_redhat5 run "povray +I4_cubes.pov +Oframe000.png +K.0  -H5000 -W5000"
['/home/hmeng/git-development/cctools/umbrella/4_cubes.pov', '/home/hmeng/git-development/cctools/umbrella/WRC_RubiksCube.inc']
/home/hmeng/umbrella_test/1
Specification_process start: 2014-10-20 22:38:35.144031
Execution environment checking ...
The information of the host machine is:
Linux hmeng 3.16.4-1-ARCH #1 SMP PREEMPT Mon Oct 6 08:22:27 CEST 2014 x86_64  
execution_environment_check end: 2014-10-20 22:38:35.150795
Installing software dependencies ...
dependency_process(povray) start: 2014-10-20 22:38:35.150875
Download software from https://www3.nd.edu/~ccl/research/data/hep-case-study/povray-x86_64-redhat5.tar.gz into the dir (/home/hmeng/umbrella_test/cache/povray-x86_64-redhat5)
dependency_process(povray) end: 2014-10-20 22:38:35.392052
linux_disto: redhat5
host_linux_distro: arch
The required linux distro specified in the specification is redhat5; the linux distro of the host machine is arch. The redhat5 os images will be downloaded.
dependency_process(redhat) start: 2014-10-20 22:38:35.392162
Download software from https://www3.nd.edu/~ccl/research/data/hep-case-study/redhat-5.10-x86_64.tar.gz into the dir (/home/hmeng/umbrella_test/cache/redhat-5.10-x86_64)
dependency_process(redhat) end: 2014-10-20 22:39:44.681829
dependency_process(cctools) start: 2014-10-20 22:39:44.685722
Download software from https://www3.nd.edu/~ccl/research/data/hep-case-study/cctools-x86_64-arch.tar.gz into the dir (/home/hmeng/umbrella_test/cache/cctools-x86_64-arch)
dependency_process(cctools) end: 2014-10-20 22:39:45.690603
software_dependencies_installation end: 2014-10-20 22:39:45.690648
workflow_repeat
local-parrot pre-processing start: 2014-10-20 22:39:45.690681
uid: 1000(hmeng); gid: 100(users)
{u'/software/povray-x86_64-redhat5': u'/home/hmeng/umbrella_test/cache/povray-x86_64-redhat5', '/': u'/home/hmeng/umbrella_test/cache/redhat-5.10-x86_64'}
need to create rootfs
hmeng is included in /etc/passwd!
users is included in /etc/group!
chroot_path_env 
{u'/software/povray-x86_64-redhat5': u'/home/hmeng/umbrella_test/cache/povray-x86_64-redhat5'}
The environment variables:
{'LESSOPEN': '|/usr/bin/lesspipe.sh %s', 'SSH_CLIENT': '74.94.121.59 58370 22', 'CVS_RSH': 'ssh', 'LOGNAME': 'root', 'USER': 'hmeng', 'INPUTRC': '/etc/inputrc', 'HOME': '/home/hmeng/umbrella_test/1/hmeng', 'PATH': u'/software/povray-x86_64-redhat5/bin:/usr/kerberos/sbin:/usr/kerberos/bin:/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin:/root/bin', 'LANG': 'en_US.UTF-8', 'TERM': 'xterm-color', 'SHELL': '/bin/bash', 'SHLVL': '1', 'G_BROKEN_FILENAMES': '1', 'HISTSIZE': '1000', 'PARROT_MOUNT_FILE': '/home/hmeng/umbrella_test/1/mountlist', 'PARROT_LDSO_PATH': u'/home/hmeng/umbrella_test/cache/redhat-5.10-x86_64/lib64/ld-linux-x86-64.so.2', '_': '/bin/env', 'SSH_CONNECTION': '74.94.121.59 58370 129.74.246.188 22', 'SSH_TTY': '/dev/pts/0', 'OLDPWD': '/root', 'HOSTNAME': 'ndcloudvm041', 'PWD': '/home/hmeng/umbrella_test/1', 'MAIL': '/var/spool/mail/root', 'LS_COLORS': 'no=00:fi=00:di=01;34:ln=01;36:pi=40;33:so=01;35:bd=40;33;01:cd=40;33;01:or=01;05;37;41:mi=01;05;37;41:ex=01;32:*.cmd=01;32:*.exe=01;32:*.com=01;32:*.btm=01;32:*.bat=01;32:*.sh=01;32:*.csh=01;32:*.tar=01;31:*.tgz=01;31:*.arj=01;31:*.taz=01;31:*.lzh=01;31:*.zip=01;31:*.z=01;31:*.Z=01;31:*.gz=01;31:*.bz2=01;31:*.bz=01;31:*.tz=01;31:*.rpm=01;31:*.cpio=01;31:*.jpg=01;35:*.gif=01;35:*.bmp=01;35:*.xbm=01;35:*.xpm=01;35:*.png=01;35:*.tif=01;35:'}

local-parrot download successfulness testing start: 2014-10-20 22:39:45.691282

Here are some tests to illustrate the cctools and os images are downloaded********
du -hs /home/hmeng/umbrella_test/cache/cctools-x86_64-arch
21M     /home/hmeng/umbrella_test/cache/cctools-x86_64-arch

ls /home/hmeng/umbrella_test/cache/redhat-5.10-x86_64
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

local-parrot download successfulness testing end: 2014-10-20 22:39:45.712493
local-parrot pre-processing end: 2014-10-20 22:39:45.712569
povray: cannot open the system configuration file /usr/local/etc/povray/3.6/povray.conf: No such file or directory
povray: cannot open the user configuration file /home/hmeng/umbrella_test/1/hmeng/.povray/3.6/povray.conf: No such file or directory
povray: I/O restrictions are disabled.


Total Scene Processing Times
  Parse Time:    0 hours  0 minutes  1 seconds (1 seconds)
  Photon Time:   0 hours  0 minutes  0 seconds (0 seconds)
  Render Time:   0 hours 24 minutes 43 seconds (1483 seconds)
  Total Time:    0 hours 24 minutes 44 seconds (1484 seconds)
local-parrot user_cmd execution end: 2014-10-20 23:04:29.033171
Rename the sandbox dir(/home/hmeng/umbrella_test/1) to the output directory(/home/hmeng/umbrella_test/parrot_povray_redhat5)
local-parrot post processing end: 2014-10-20 23:04:29.033251
workflow_repeat end: 2014-10-20 23:04:29.033275



