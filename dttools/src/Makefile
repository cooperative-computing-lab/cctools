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
	bucketing.c \
	bucketing_exhaust.c \
	bucketing_greedy.c \
	bucketing_manager.c \
	buffer.c \
	catalog_query.c \
	category.c \
	cctools.c \
	cctools_endian.c \
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
	file_link_recursive.c \
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
	jx_binary.c\
	jx_getopt.c \
	jx_match.c \
	jx_parse.c \
	jx_print.c \
	jx_pretty_print.c \
	jx_canonicalize.c \
	jx_table.c \
	jx_eval.c \
	jx_sub.c \
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
	ppoll_compat.c \
	preadwrite.c \
	priority_queue.c \
	priority_queue_test.c \
	priority_map.c \
	priority_map_test.c \
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
	tlq_config.c \
	trash.c \
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
	bitmap.h \
	bucketing.h \
	bucketing_exhaust.h \
	bucketing_greedy.h \
	bucketing_manager.h \
	buffer.h \
	category.h \
	cctools.h \
	copy_tree.h \
	compat-at.h \
	debug.h \
	envtools.h \
	fast_popen.h \
	file_link_recursive.h \
	full_io.h \
	getopt.h \
	getopt_aux.h \
	hash_table.h \
	histogram.h \
	host_memory_info.h \
	http_query.h \
	int_sizes.h \
	itable.h \
	jx.h \
	jx_match.h \
	link.h \
	list.h \
	load_average.h \
	md5.h \
	macros.h \
	path.h \
	priority_queue.h \
	rmonitor_poll.h \
	rmsummary.h \
	stringtools.h \
	text_array.h \
	text_list.h \
	timestamp.h \
	unlink_recursive.h \
	xxmalloc.h \

LIBRARIES = libdttools.a

ifneq ($(CCTOOLS_STATIC),1)
PRELOAD_LIBRARIES = libforce_halt_enospc.so
endif

ifeq ($(CCTOOLS_CURL_AVAILABLE),yes)
CCTOOLS_EXTERNAL_LINKAGE += $(CCTOOLS_CURL_LDFLAGS) -lssl -lcrypto
SOURCES += s3_file_io.c
HEADERS_PUBLIC += s3_file_io.h
endif

OBJECTS = $(SOURCES:%.c=%.o)

#separate, because catalog_query has a slightly different order of linking.
MOST_PROGRAMS = catalog_update watchdog disk_allocator jx2json jx2env jx_repl env_replace
PROGRAMS = $(MOST_PROGRAMS) catalog_query

SCRIPTS = cctools_gpu_autodetect
TARGETS = $(LIBRARIES) $(PRELOAD_LIBRARIES) $(PROGRAMS) $(TEST_PROGRAMS)
TEST_PROGRAMS = auth_test disk_alloc_test jx_test microbench multirun jx_count_obj_test jx_canonicalize_test jx_merge_test hash_table_offset_test hash_table_fromkey_test histogram_test category_test jx_binary_test bucketing_base_test bucketing_manager_test priority_queue_test priority_map_test

all: $(TARGETS) catalog_query

libdttools.a: $(OBJECTS)

$(MOST_PROGRAMS) $(TEST_PROGRAMS): libdttools.a

jx_parse.o: jx_parse.c
	$(CCTOOLS_CC) -O3 -o $@ -c $(CCTOOLS_INTERNAL_CCFLAGS) $(LOCAL_CCFLAGS) $<

jx.o: jx.c
	$(CCTOOLS_CC) -O3 -o $@ -c $(CCTOOLS_INTERNAL_CCFLAGS) $(LOCAL_CCFLAGS) $<

jx_repl: jx_repl.o libdttools.a
	$(CCTOOLS_LD) -o $@ $(CCTOOLS_INTERNAL_LDFLAGS) $(LOCAL_LDFLAGS) $^ $(LOCAL_LINKAGE) $(CCTOOLS_EXTERNAL_LINKAGE) $(CCTOOLS_READLINE_LDFLAGS)

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

lint:
		if ( ! clang-format -Werror --dry-run --style='file:../../.clang-format' $(SOURCES));\
		then\
				echo  "========================================================";\
				echo  "NOTICE: Run `make format` to format your latest changes.";\
				echo  "========================================================";\
				exit 1;\
		fi

format:
		clang-format -i $(SOURCES)


.PHONY: all clean install test

# vim: set noexpandtab tabstop=4:
