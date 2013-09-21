ifdef(`HTML',`include(manual_html.h)',`include(manual_man.h)')dnl
changecom(`@@')dnl
define(COPYRIGHT_BOILERPLATE,The Cooperative Computing Tools are Copyright (C) 2003-2004 Douglas Thain and Copyright (C) 2005-2011 The University of Notre Dame.  This software is distributed under the GNU General Public License.  See the file COPYING for details.)dnl
dnl
define(SEE_ALSO_MAKEFLOW,
`LIST_BEGIN
LIST_ITEM MANUAL(Cooperative Computing Tools Documentation,"../index.html")
LIST_ITEM MANUAL(Makeflow User Manual,"../makeflow.html")
LIST_ITEM MANPAGE(makeflow,1)
LIST_ITEM MANPAGE(makeflow_monitor,1)
LIST_ITEM MANPAGE(starch,1)
LIST_END')dnl
dnl
define(SEE_ALSO_WORK_QUEUE,
`LIST_BEGIN
LIST_ITEM MANUAL(Cooperative Computing Tools Documentation,"../index.html")
LIST_ITEM MANUAL(Work Queue User Manual,"../workqueue.html")
LIST_ITEM MANPAGE(work_queue_worker,1)
LIST_ITEM MANPAGE(work_queue_status,1)
LIST_ITEM MANPAGE(work_queue_pool,1)
LIST_ITEM MANPAGE(condor_submit_workers,1)
LIST_ITEM MANPAGE(sge_submit_workers,1)
LIST_ITEM MANPAGE(torque_submit_workers,1)
LIST_ITEM MANPAGE(ec2_submit_workers,1)
LIST_ITEM MANPAGE(ec2_remove_workers,1)
LIST_END')dnl
dnl
define(SEE_ALSO_PARROT,
`LIST_BEGIN
LIST_ITEM MANUAL(Cooperative Computing Tools Documentation,"../index.html")
LIST_ITEM MANUAL(Parrot User Manual,"../parrot.html")
LIST_ITEM MANPAGE(parrot_run,1)
LIST_ITEM MANPAGE(parrot_run_hdfs,1)
LIST_ITEM MANPAGE(parrot_cp,1)
LIST_ITEM MANPAGE(parrot_getacl,1)
LIST_ITEM MANPAGE(parrot_setacl,1)
LIST_ITEM MANPAGE(parrot_mkalloc,1)
LIST_ITEM MANPAGE(parrot_lsalloc,1)
LIST_ITEM MANPAGE(parrot_locate,1)
LIST_ITEM MANPAGE(parrot_timeout,1)
LIST_ITEM MANPAGE(parrot_whoami,1)
LIST_ITEM MANPAGE(parrot_md5,1)
LIST_END')dnl
define(SEE_ALSO_CHIRP,
`LIST_BEGIN
LIST_ITEM MANUAL(Cooperative Computing Tools Documentation,"../index.html")
LIST_ITEM MANUAL(Chirp User Manual,"../chirp.html")
LIST_ITEM MANPAGE(chirp,1)
LIST_ITEM MANPAGE(chirp_status,1)
LIST_ITEM MANPAGE(chirp_fuse,1)
LIST_ITEM MANPAGE(chirp_get,1)
LIST_ITEM MANPAGE(chirp_put,1)
LIST_ITEM MANPAGE(chirp_stream_files,1)
LIST_ITEM MANPAGE(chirp_distribute,1)
LIST_ITEM MANPAGE(chirp_benchmark,1)
LIST_ITEM MANPAGE(chirp_server,1)
LIST_ITEM MANPAGE(chirp_server_hdfs,1)
LIST_END')dnl
dnl
define(SEE_ALSO_SAND,
`LIST_BEGIN
LIST_ITEM MANUAL(Cooperative Computing Tools Documentation,"../index.html")
LIST_ITEM MANUAL(SAND User Manual,"../sand.html")
LIST_ITEM MANPAGE(sand_filter_master,1)
LIST_ITEM MANPAGE(sand_filter_kernel,1)
LIST_ITEM MANPAGE(sand_align_master,1)
LIST_ITEM MANPAGE(sand_align_kernel,1)
LIST_ITEM MANPAGE(sand_compress_reads,1)
LIST_ITEM MANPAGE(sand_uncompress_reads,1)
LIST_ITEM MANPAGE(work_queue_worker,1)
LIST_END')dnl
define(SEE_ALSO_LINKER,
`LIST_BEGIN
LIST_ITEM MANPAGE(makeflow,1)
LIST_ITEM MANPAGE(perl,1)
LIST_ITEM MANPAGE(python,1)
LIST_ITEM MANPAGE(ldd, 1)
LIST_END')dnl
dnl
