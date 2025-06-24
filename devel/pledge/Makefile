CC = gcc
CFLAGS = -Wall -Wextra -Wpedantic
CFLAGS += -fpic
# some posix functions werent added to standard c until c23, so we have
# to specify
# POSIX_C_SOURCE is saying that we want to enable functionality from 2008 posix
CFLAGS += -std=c11 -D_POSIX_C_SOURCE=200809L
# Alternatively, this enables extensions and all functionality
#CFLAGS += -std=gnu11

RELFLGS = -O3

LDFLAGS += -ldl -shared

BUILDDIR = ./build
TARGET = $(BUILDDIR)/libenforcer.so

SRCS = src/enforcer.c
OBJ = $(BUILDDIR)/enforcer.o

all: $(TARGET)

$(TARGET): $(OBJ)
	$(CC) $(CFLAGS) $(LDFLAGS) $(OBJ) -o $(TARGET)

# Order only build dir
$(OBJ) : $(SRCS) | $(BUILDDIR)
	$(CC) $(CFLAGS) -c $< -o $@

$(BUILDDIR):
	mkdir $@

.PHONY : clean all
clean:
	rm -rf $(BUILDDIR)
