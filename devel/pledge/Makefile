include ../../config.mk
include ../../rules.mk

INCDIR = ./src
INCDIR += -I$(CCTOOLS_HOME)/dttools/src
BUILDDIR = ./build

CC = gcc
LOCAL_CFLAGS = -Wall -Wextra -Wpedantic
LOCAL_CFLAGS += -fdiagnostics-color=always

LDFLAGS += -lm

# If we want to color output while enforcing
# DEFS = -DCOLOR_ENFORCING

# some posix functions werent added to standard c until c23, so we have
# to specify
# POSIX_C_SOURCE is saying that we want to enable functionality from 2008 posix
# LOCAL_CFLAGS += -std=c11 -D_POSIX_C_SOURCE=200809L
# Alternatively, this enables extensions and all functionality
LOCAL_CFLAGS += -std=gnu11

LOCAL_CFLAGS += -I$(INCDIR)

RELFLGS = -O3

# LDFLAGS += $(CCTOOLS_HOME)/dttools/src/libdttools.a

# COMMON_SRCS = src/pl_list.c src/util.c
COMMON_SRCS = src/list_util.c src/util.c
COMMON_SRCS += $(CCTOOLS_HOME)/dttools/src/libdttools.a

#####################################################################
# Enforcer
#####################################################################
ENFORCER = $(BUILDDIR)/libenforcer.so
ENF_LDFLAGS += $(LDFLAGS) -ldl -shared
ENF_LOCAL_CFLAGS += $(LOCAL_CFLAGS) -fPIC
ENF_SRCS = src/enforcer.c
ENF_OBJ = $(BUILDDIR)/enforcer.o

ENFORCER_H = src/libenforcer.h

#####################################################################
# PLEDGE
#####################################################################
PLEDGE = $(BUILDDIR)/pledge
PL_LDFLAGS += $(LDFLAGS)
PL_LOCAL_CFLAGS += $(LOCAL_CFLAGS)
PL_SRCS = src/pledge.c
PL_OBJ = $(BUILDDIR)/pledge.o

all: $(PLEDGE)

$(PLEDGE): $(ENFORCER_H) $(PL_OBJ)
	$(CC) $(PL_LOCAL_CFLAGS) $(PL_OBJ) $(COMMON_SRCS) -o $(PLEDGE) $(PL_LDFLAGS)

$(ENFORCER_H): $(ENFORCER)
	xxd -i -n minienforcer $(BUILDDIR)/libenforcer.so > src/libenforcer.h

$(ENFORCER): $(ENF_OBJ)
	$(CC) $(ENF_LOCAL_CFLAGS) $(ENF_OBJ) $(COMMON_SRCS) -o $(ENFORCER) $(ENF_LDFLAGS)

$(PL_OBJ) : $(PL_SRCS) | $(BUILDDIR)
	$(CC) $(PL_LOCAL_CFLAGS) -c $< -o $@

# # Order only build dir
$(ENF_OBJ) : $(ENF_SRCS) | $(BUILDDIR)
	$(CC) $(ENF_LOCAL_CFLAGS) $(DEFS) -c $< -o $@

$(BUILDDIR):
	mkdir $@

.PHONY : clean all
clean:
	rm -rf $(BUILDDIR)
	rm src/libenforcer.h
