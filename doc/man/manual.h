ifdef(`HTML',`include(manual_html.h)')dnl
ifdef(`MD',`include(manual_md.h)')dnl
ifdef(`MAN',`include(manual_man.h)')dnl
changecom(`@@')dnl
define(COPYRIGHT_BOILERPLATE,The Cooperative Computing Tools are Copyright (C) 2022 The University of Notre Dame.  This software is distributed under the GNU General Public License.  See the file COPYING for details.)dnl
dnl
define(SEE_ALSO_MAKEFLOW,
`LIST_BEGIN
LIST_ITEM(MANUAL(Cooperative Computing Tools Documentation,"../index.html"))
LIST_ITEM(MANUAL(Makeflow User Manual,"../makeflow.html"))
LIST_ITEM(MANPAGE(makeflow,1) MANPAGE(makeflow_monitor,1) MANPAGE(makeflow_analyze,1) MANPAGE(makeflow_viz,1) MANPAGE(makeflow_graph_log,1) MANPAGE(starch,1) MANPAGE(makeflow_ec2_setup,1) MANPAGE(makeflow_ec2_cleanup,1))
LIST_END')dnl
dnl
define(SEE_ALSO_WORK_QUEUE,
`LIST_BEGIN
LIST_ITEM(MANUAL(Cooperative Computing Tools Documentation,"../index.html"))
LIST_ITEM(MANUAL(Work Queue User Manual,"../workqueue.html"))
LIST_ITEM(MANPAGE(work_queue_worker,1) MANPAGE(work_queue_status,1) MANPAGE(work_queue_factory,1) MANPAGE(condor_submit_workers,1) MANPAGE(sge_submit_workers,1) MANPAGE(torque_submit_workers,1) )
LIST_END')dnl
dnl
define(SEE_ALSO_TASK_VINE,
`LIST_BEGIN
LIST_ITEM(MANUAL(Cooperative Computing Tools Documentation,"../index.html"))
LIST_ITEM(MANUAL(TaskVine User Manual,"../taskvine.html"))
LIST_ITEM(MANPAGE(vine_worker,1) MANPAGE(vine_status,1) MANPAGE(vine_factory,1) MANPAGE(vine_graph_log,1) )
LIST_END')dnl
dnl
define(SEE_ALSO_PARROT,
`LIST_BEGIN
LIST_ITEM(MANUAL(Cooperative Computing Tools Documentation,"../index.html"))
LIST_ITEM(MANUAL(Parrot User Manual,"../parrot.html"))
LIST_ITEM(MANPAGE(parrot_run,1) MANPAGE(parrot_cp,1) MANPAGE(parrot_getacl,1)  MANPAGE(parrot_setacl,1)  MANPAGE(parrot_mkalloc,1)  MANPAGE(parrot_lsalloc,1)  MANPAGE(parrot_locate,1)  MANPAGE(parrot_timeout,1)  MANPAGE(parrot_whoami,1)  MANPAGE(parrot_mount,1)  MANPAGE(parrot_md5,1)  MANPAGE(parrot_package_create,1)  MANPAGE(parrot_package_run,1)  MANPAGE(chroot_package_run,1))
LIST_END')dnl
define(SEE_ALSO_CHIRP,
`LIST_BEGIN
LIST_ITEM(MANUAL(Cooperative Computing Tools Documentation,"../index.html"))
LIST_ITEM(MANUAL(Chirp User Manual,"../chirp.html"))
LIST_ITEM(MANPAGE(chirp,1)  MANPAGE(chirp_status,1)  MANPAGE(chirp_fuse,1)  MANPAGE(chirp_get,1)  MANPAGE(chirp_put,1)  MANPAGE(chirp_stream_files,1)  MANPAGE(chirp_distribute,1)  MANPAGE(chirp_benchmark,1)  MANPAGE(chirp_server,1))
LIST_END')dnl
dnl
define(SEE_ALSO_SAND,
`LIST_BEGIN
LIST_ITEM(MANUAL(Cooperative Computing Tools Documentation,"../index.html"))
LIST_ITEM(MANUAL(SAND User Manual,"../sand.html"))
LIST_ITEM(MANPAGE(sand_filter_master,1)  MANPAGE(sand_filter_kernel,1)  MANPAGE(sand_align_master,1)  MANPAGE(sand_align_kernel,1)  MANPAGE(sand_compress_reads,1)  MANPAGE(sand_uncompress_reads,1)  MANPAGE(work_queue_worker,1))
LIST_END')dnl
define(SEE_ALSO_LINKER,
`LIST_BEGIN
LIST_ITEM MANPAGE(makeflow,1) perl(1), python(1), ldd(1)
LIST_END')dnl
define(SEE_ALSO_CATALOG,
`LIST_BEGIN
LIST_ITEM(MANUAL(Cooperative Computing Tools Documentation,"../index.html"))
LIST_ITEM(MANPAGE(catalog_server,1)  MANPAGE(catalog_update,1)  MANPAGE(catalog_query,1)  MANPAGE(chirp_status,1)  MANPAGE(work_queue_status,1)   MANPAGE(deltadb_query,1))
LIST_END')dnl
dnl
