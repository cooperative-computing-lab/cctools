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
	b64_encode.c \
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
	disk_info.c \
	domain_name.c \
	domain_name_cache.c \
	dpopen.c \
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
	hmac.c \
	http_query.c \
	itable.c \
	json.c \
	json_aux.c \
	link.c \
	link_auth.c \
	link_nvpair.c \
	list.c \
	load_average.c \
	md5.c \
	memory_info.c \
	mergesort.c \
	mpi_queue.c \
	nvpair.c \
	nvpair_database.c \
	password_cache.c \
	path.c \
	preadwrite.c \
	process.c \
	random_init.c \
	rmonitor.c \
	rmsummary.c \
	set.c \
	sha1.c \
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
	timestamp.h

LIBRARIES = libdttools.a
OBJECTS = $(SOURCES:%.c=%.o)
PROGRAMS = catalog_update catalog_server watchdog
SCRIPTS = cctools_gpu_autodetect
TARGETS = $(LIBRARIES) $(PROGRAMS) $(TEST_PROGRAMS)
TEST_PROGRAMS = auth_test microbench multirun

all: $(TARGETS)

libdttools.a: $(OBJECTS)
$(PROGRAMS) $(TEST_PROGRAMS): $(LIBRARIES)

test: all

clean:
	rm -f $(OBJECTS) $(TARGETS)

install: all
	mkdir -p $(CCTOOLS_INSTALL_DIR)/bin
	cp $(SCRIPTS) $(PROGRAMS) $(CCTOOLS_INSTALL_DIR)/bin/
	mkdir -p $(CCTOOLS_INSTALL_DIR)/lib
	cp $(LIBRARIES) $(CCTOOLS_INSTALL_DIR)/lib/
	mkdir -p $(CCTOOLS_INSTALL_DIR)/include/cctools
	cp $(HEADERS_PUBLIC) $(CCTOOLS_INSTALL_DIR)/include/cctools/
