include ../../../config.mk
include ../../../rules.mk

PROGRAMS = pledge
LIBRARIES = libenforcer.so

TARGETS = $(LIBRARIES) $(PROGRAMS)

EXTERNAL_DEPENDENCIES = $(CCTOOLS_HOME)/dttools/src/libdttools.a

SOURCES = pledge.c list_util.c util.c enforcer.c
COMMON_SRCS = list_util.c util.c

OBJECTS = list_util.o util.o pledge.o enforcer.o
OBJECTS_PLEDGE = list_util.o util.o pledge.o

# If we want to color output while enforcing
# DEFS = -DCOLOR_ENFORCING

all: $(TARGETS)

$(PROGRAMS): $(EXTERNAL_DEPENDENCIES)

pledge: $(OBJECTS_PLEDGE) $(EXTERNAL_DEPENDENCIES)

libenforcer.$(CCTOOLS_DYNAMIC_SUFFIX): enforcer.o $(EXTERNAL_DEPENDENCIES) $(COMMON_SRCS)
	$(CCTOOLS_CC) $(CCTOOLS_BASE_CCFLAGS) $(CCTOOLS_INTERNAL_CCFLAGS) -ldl -shared -o $@ $^
	xxd -i -n minienforcer libenforcer.so > libenforcer.h


enforcer.o: enforcer.c
	 $(CCTOOLS_CC) -o $@ -c $(CCTOOLS_INTERNAL_CCFLAGS) $(LOCAL_CCFLAGS) $(DEFS) $<

format:
	clang-format -i $(SOURCES)

clean:
	rm $(OBJECTS) $(TARGETS) libenforcer.h

.PHONY: all clean install test lint format bindings
