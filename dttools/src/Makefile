include ../../config.mk
include ../../rules.mk

SOURCES = \
	address.c \
	auth.c \
	auth_address.c \
	auth_all.c \
	auth_globus.c \
	auth_hostname.c \
	auth_kerberos.c \
	auth_ticket.c \
	auth_unix.c \
	b64.c \
	bitmap.c \
	buffer.c \
	catalog_query.c \
	category.c \
	cctools.c \
	change_process_title.c \
	chunk.c \
	clean_dir.c \
	compat-at.c \
	console_login.c \
	copy_stream.c \
	copy_tree.c \
	create_dir.c \
	daemon.c \
	datagram.c \
	debug.c \
	debug_file.c \
	debug_journal.c \
	debug_stream.c \
	debug_syslog.c \
	disk_alloc.c \
	domain_name.c \
	domain_name_cache.c \
	dpopen.c \
	elfheader.c \
	envtools.c \
	fast_popen.c \
	fd.c \
	file_cache.c \
	full_io.c \
	get_canonical_path.c \
	get_line.c \
	getopt.c \
	getopt_aux.c \
	gpu_info.c \
	hash_cache.c \
	hash_table.c \
	hdfs_library.c \
	histogram.c \
	hmac.c \
	host_disk_info.c \
	host_memory_info.c \
	http_query.c \
	interfaces_address.c \
	itable.c \
	jx.c \
	jx_multi_database.c \
	jx_database.c \
	jx_getopt.c \
	jx_match.c \
	jx_parse.c \
	jx_print.c \
	jx_pretty_print.c \
	jx_canonicalize.c \
	jx_table.c \
	jx_export.c \
	jx_eval.c \
	jx_function.c \
	link.c \
	link_auth.c \
	list.c \
	load_average.c \
	md5.c \
	memfdexe.c \
	mkdir_recursive.c \
	twister.c \
	nvpair.c \
	nvpair_jx.c \
	password_cache.c \
	path.c \
	path_disk_size_info.c \
	pattern.c \
	preadwrite.c \
	process.c \
	random.c \
	rmonitor.c \
	rmonitor_poll.c \
	rmsummary.c \
	set.c \
    semaphore.c \
	sha1.c \
	shell.c \
	sh_popen.c\
	sigdef.c \
	sleeptools.c \
	sort_dir.c \
	stats.c \
	string_array.c \
	stringtools.c \
	string_set.c \
	text_array.c \
	text_list.c \
	timer.c \
	timestamp.c \
	unlink_recursive.c \
	uptime.c \
	url_encode.c \
	username.c \
	uuid.c \
	xxmalloc.c \

HEADERS_PUBLIC = \
	auth.h \
	auth_address.h \
	auth_all.h \
	auth_globus.h \
	auth_hostname.h \
	auth_kerberos.h \
	auth_ticket.h \
	auth_unix.h \
	buffer.h \
	category.h \
	copy_tree.h \
	debug.h \
	hash_table.h \
	histogram.h \
	http_query.h \
	int_sizes.h \
	jx.h \
	jx_match.h \
	link.h \
	md5.h \
	rmonitor_poll.h \
	rmsummary.h \
	timestamp.h \

LIBRARIES = libdttools.a
PRELOAD_LIBRARIES = libforce_halt_enospc.so
ifeq ($(CCTOOLS_CURL_AVAILABLE),yes)
CCTOOLS_EXTERNAL_LINKAGE += $(CCTOOLS_CURL_LDFLAGS) -lssl -lcrypto
SOURCES += s3_file_io.c
HEADERS_PUBLIC += s3_file_io.h
endif

OBJECTS = $(SOURCES:%.c=%.o)

#separate, because catalog_query has a slightly different order of linking.
MOST_PROGRAMS = catalog_update catalog_server watchdog disk_allocator jx2json jx2env
PROGRAMS = $(MOST_PROGRAMS) catalog_query

SCRIPTS = cctools_gpu_autodetect cctools_python
TARGETS = $(LIBRARIES) $(PRELOAD_LIBRARIES) $(PROGRAMS) $(TEST_PROGRAMS)
TEST_PROGRAMS = auth_test disk_alloc_test jx_test microbench multirun jx_count_obj_test histogram_test category_test

all: $(TARGETS) catalog_query

libdttools.a: $(OBJECTS)

$(MOST_PROGRAMS) $(TEST_PROGRAMS): libdttools.a

catalog_query: catalog_query.o catalog_query_tool.o libdttools.a

auth_globus.o: auth_globus.c
	$(CCTOOLS_CC) -o $@ -c $(CCTOOLS_INTERNAL_CCFLAGS) $(LOCAL_CCFLAGS) $(CCTOOLS_GLOBUS_CCFLAGS) $<

auth_test: auth_test.o libdttools.a
ifeq ($(CCTOOLS_STATIC),1)
	@echo "auth_test currently cannot be build statically."
else
	$(CCTOOLS_LD) -o $@ $(CCTOOLS_INTERNAL_LDFLAGS) $(LOCAL_LDFLAGS) $^ $(LOCAL_LINKAGE) $(CCTOOLS_GLOBUS_LDFLAGS) $(CCTOOLS_EXTERNAL_LINKAGE)
endif

libforce_halt_enospc.so: libforce_halt_enospc.o
	$(CCTOOLS_CC) -shared -fPIC $^ -o $@ -ldl

clean:
	rm -f $(OBJECTS) $(TARGETS) *.o

install: all
	mkdir -p $(CCTOOLS_INSTALL_DIR)/bin
	cp $(SCRIPTS) $(PROGRAMS) $(CCTOOLS_INSTALL_DIR)/bin/
	mkdir -p $(CCTOOLS_INSTALL_DIR)/lib
	cp $(LIBRARIES) $(PRELOAD_LIBRARIES) $(CCTOOLS_INSTALL_DIR)/lib/
	mkdir -p $(CCTOOLS_INSTALL_DIR)/include/cctools
	cp $(HEADERS_PUBLIC) $(CCTOOLS_INSTALL_DIR)/include/cctools/

test: all

.PHONY: all clean install test

# vim: set noexpandtab tabstop=4:
