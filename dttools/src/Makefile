include ../../config.mk
include ../../rules.mk

SOURCES = \
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
	cctools.c \
	change_process_title.c \
	chunk.c \
	clean_dir.c \
	console_login.c \
	copy_stream.c \
	create_dir.c \
	daemon.c \
	datagram.c \
	debug.c \
	debug_file.c \
	debug_journal.c \
	debug_stream.c \
	debug_syslog.c \
	disk_alloc.c \
	dpopen.c \
	elfheader.c \
	envtools.c \
	fast_popen.c \
	fd.c \
	file_cache.c \
	full_io.c \
	get_canonical_path.c \
	get_line.c \
	getaddrinfo_cache.c \
	getopt.c \
	getopt_aux.c \
	gpu_info.c \
	hash_cache.c \
	hash_table.c \
	hdfs_library.c \
	hmac.c \
	host_disk_info.c \
	host_memory_info.c \
	hostname.c \
	http_query.c \
	itable.c \
	json.c \
	json_aux.c \
	jx.c \
	jx_parse.c \
	jx_print.c \
	jx_table.c \
	link.c \
	link_auth.c \
	link_nvpair.c \
	list.c \
	load_average.c \
	md5.c \
	memfdexe.c \
	mergesort.c \
	mt19937-64.c \
	nvpair.c \
	nvpair_database.c \
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
	sha1.c \
	shell.c \
	sigdef.c \
	sleeptools.c \
	sort_dir.c \
	string_array.c \
	stringtools.c \
	text_array.c \
	text_list.c \
	timer.c \
	timestamp.c \
	unlink_recursive.c \
	uptime.c \
	url_encode.c \
	username.c \
	xxmalloc.c

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
	debug.h \
	int_sizes.h \
	link.h \
	md5.h \
	rmonitor_poll.h \
	rmsummary.h \
	timestamp.h

LIBRARIES = libdttools.a
OBJECTS = $(SOURCES:%.c=%.o)
PROGRAMS = catalog_update catalog_server watchdog disk_allocator
SCRIPTS = cctools_gpu_autodetect cctools_python
TARGETS = $(LIBRARIES) $(PROGRAMS) $(TEST_PROGRAMS)
TEST_PROGRAMS = auth_test disk_alloc_test jx_test microbench multirun

all: $(TARGETS)

libdttools.a: $(OBJECTS)

$(PROGRAMS) $(TEST_PROGRAMS): $(LIBRARIES)

auth_globus.o: auth_globus.c
	$(CCTOOLS_CC) -o $@ -c $(CCTOOLS_INTERNAL_CCFLAGS) $(LOCAL_CCFLAGS) $(CCTOOLS_GLOBUS_CCFLAGS) $<

auth_test: auth_test.o libdttools.a
	$(CCTOOLS_LD) -o $@ $(CCTOOLS_INTERNAL_LDFLAGS) $(LOCAL_LDFLAGS) $^ $(LOCAL_LINKAGE) $(CCTOOLS_GLOBUS_LDFLAGS) $(CCTOOLS_EXTERNAL_LINKAGE)

clean:
	rm -f $(OBJECTS) $(TARGETS) auth_test.o

install: all
	mkdir -p $(CCTOOLS_INSTALL_DIR)/bin
	cp $(SCRIPTS) $(PROGRAMS) $(CCTOOLS_INSTALL_DIR)/bin/
	mkdir -p $(CCTOOLS_INSTALL_DIR)/lib
	cp $(LIBRARIES) $(CCTOOLS_INSTALL_DIR)/lib/
	mkdir -p $(CCTOOLS_INSTALL_DIR)/include/cctools
	cp $(HEADERS_PUBLIC) $(CCTOOLS_INSTALL_DIR)/include/cctools/

test: all

.PHONY: all clean install test

# vim: set noexpandtab tabstop=4:
