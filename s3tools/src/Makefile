include ../../config.mk
include ../../rules.mk

EXTERNAL_DEPENDENCIES = ../../dttools/src/libdttools.a
LIBRARIES = libs3client.a
OBJECTS = $(SOURCES:%.c=%.o)
PROGRAMS = s3put s3get s3stat s3rm s3mkdir s3rmdir s3ls s3getacl s3setacl
SOURCES = s3c_file.c s3c_acl.c s3c_bucket.c s3c_util.c
TARGETS = $(LIBRARIES) $(PROGRAMS)

all: $(TARGETS)

libs3client.a: $(OBJECTS)
$(PROGRAMS): s3common.o $(LIBRARIES) $(EXTERNAL_DEPENDENCIES)

clean:
	rm -f $(OBJECTS) $(TARGETS)

install:

test:
