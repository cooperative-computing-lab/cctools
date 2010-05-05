# Makefile
# http://www.gnu.org/software/make/manual/make.html

CC = gcc

#flags for compiling the executable
CFLAGS = -Wall -pedantic -g -O3


SOURCE = s3tool.cpp aws_s3.cpp aws_s3_misc.cpp

INCLUDEDIRS =

# defined HAVE_CONFIG_H to make curlpp work
DEFINES = -DHAVE_CONFIG_H

LIBS = -lstdc++ -lssl -lcrypto -lcurlpp -lcurl

EXECNAME = s3tool


CSOURCES = $(filter %.c,$(SOURCE))
CPPSOURCES = $(filter %.cpp,$(SOURCE))
MSOURCES = $(filter %.m,$(SOURCE))

OBJECTS = $(CSOURCES:.c=.o) \
          $(CPPSOURCES:.cpp=.o) \
          $(MSOURCES:.m=.o)


CFLAGS := $(CFLAGS) $(DEFINES) $(INCLUDES) $(INCLUDEDIRS)


default: $(EXECNAME) Makefile


run: $(EXECNAME) Makefile
	./$(EXECNAME)

#$(EXECNAME): $(OBJECTS)

$(EXECNAME): $(OBJECTS)
	$(CC) $(CFLAGS) $(OBJECTS) $(LIBS) -o $(EXECNAME)


%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

%.o: %.cpp
	$(CC) $(CFLAGS) -c $< -o $@

%.o: %.m
	$(CC) $(CFLAGS) -c $< -o $@


install:


clean:
	rm -f $(OBJECTS)
	rm -f $(EXECNAME)


depend:
	makedepend -- $(CFLAGS) -- $(SOURCE)


# DO NOT DELETE THIS LINE -- makedepend depends on it.

s3tool.o: aws_s3.h multidict.h aws_s3_misc.h /usr/include/openssl/md5.h
s3tool.o: /usr/include/openssl/e_os2.h /usr/include/openssl/opensslconf.h
s3tool.o: /usr/include/stddef.h /usr/include/_types.h
s3tool.o: /usr/include/sys/_types.h /usr/include/sys/cdefs.h
s3tool.o: /usr/include/machine/_types.h /usr/include/i386/_types.h
s3tool.o: /usr/include/openssl/buffer.h /usr/include/openssl/ossl_typ.h
s3tool.o: /usr/include/sys/types.h /usr/include/sys/appleapiopts.h
s3tool.o: /usr/include/machine/types.h /usr/include/i386/types.h
s3tool.o: /usr/include/machine/endian.h /usr/include/i386/endian.h
s3tool.o: /usr/include/sys/_endian.h /usr/include/libkern/_OSByteOrder.h
s3tool.o: /usr/include/libkern/i386/_OSByteOrder.h
s3tool.o: /usr/include/sys/_structs.h /usr/include/openssl/hmac.h
s3tool.o: /usr/include/openssl/evp.h /usr/include/openssl/symhacks.h
s3tool.o: /usr/include/openssl/bio.h /usr/include/stdio.h
s3tool.o: /usr/include/secure/_stdio.h /usr/include/secure/_common.h
s3tool.o: /usr/include/stdarg.h /usr/include/openssl/crypto.h
s3tool.o: /usr/include/stdlib.h /usr/include/Availability.h
s3tool.o: /usr/include/AvailabilityInternal.h /usr/include/sys/wait.h
s3tool.o: /usr/include/sys/signal.h /usr/include/machine/signal.h
s3tool.o: /usr/include/i386/signal.h /usr/include/i386/_structs.h
s3tool.o: /usr/include/sys/resource.h /usr/include/alloca.h
s3tool.o: /usr/include/openssl/stack.h /usr/include/openssl/safestack.h
s3tool.o: /usr/include/openssl/opensslv.h /usr/include/openssl/objects.h
s3tool.o: /usr/include/openssl/obj_mac.h /usr/include/openssl/asn1.h
s3tool.o: /usr/include/time.h /usr/include/_structs.h
s3tool.o: /usr/include/openssl/bn.h /usr/include/unistd.h
s3tool.o: /usr/include/sys/unistd.h /usr/include/sys/select.h
s3tool.o: /usr/include/sys/_select.h /usr/include/pwd.h
aws_s3.o: aws_s3.h multidict.h aws_s3_misc.h /usr/include/openssl/md5.h
aws_s3.o: /usr/include/openssl/e_os2.h /usr/include/openssl/opensslconf.h
aws_s3.o: /usr/include/stddef.h /usr/include/_types.h
aws_s3.o: /usr/include/sys/_types.h /usr/include/sys/cdefs.h
aws_s3.o: /usr/include/machine/_types.h /usr/include/i386/_types.h
aws_s3.o: /usr/include/openssl/buffer.h /usr/include/openssl/ossl_typ.h
aws_s3.o: /usr/include/sys/types.h /usr/include/sys/appleapiopts.h
aws_s3.o: /usr/include/machine/types.h /usr/include/i386/types.h
aws_s3.o: /usr/include/machine/endian.h /usr/include/i386/endian.h
aws_s3.o: /usr/include/sys/_endian.h /usr/include/libkern/_OSByteOrder.h
aws_s3.o: /usr/include/libkern/i386/_OSByteOrder.h
aws_s3.o: /usr/include/sys/_structs.h /usr/include/openssl/hmac.h
aws_s3.o: /usr/include/openssl/evp.h /usr/include/openssl/symhacks.h
aws_s3.o: /usr/include/openssl/bio.h /usr/include/stdio.h
aws_s3.o: /usr/include/secure/_stdio.h /usr/include/secure/_common.h
aws_s3.o: /usr/include/stdarg.h /usr/include/openssl/crypto.h
aws_s3.o: /usr/include/stdlib.h /usr/include/Availability.h
aws_s3.o: /usr/include/AvailabilityInternal.h /usr/include/sys/wait.h
aws_s3.o: /usr/include/sys/signal.h /usr/include/machine/signal.h
aws_s3.o: /usr/include/i386/signal.h /usr/include/i386/_structs.h
aws_s3.o: /usr/include/sys/resource.h /usr/include/alloca.h
aws_s3.o: /usr/include/openssl/stack.h /usr/include/openssl/safestack.h
aws_s3.o: /usr/include/openssl/opensslv.h /usr/include/openssl/objects.h
aws_s3.o: /usr/include/openssl/obj_mac.h /usr/include/openssl/asn1.h
aws_s3.o: /usr/include/time.h /usr/include/_structs.h
aws_s3.o: /usr/include/openssl/bn.h
aws_s3_misc.o: aws_s3_misc.h /usr/include/openssl/md5.h
aws_s3_misc.o: /usr/include/openssl/e_os2.h
aws_s3_misc.o: /usr/include/openssl/opensslconf.h /usr/include/stddef.h
aws_s3_misc.o: /usr/include/_types.h /usr/include/sys/_types.h
aws_s3_misc.o: /usr/include/sys/cdefs.h /usr/include/machine/_types.h
aws_s3_misc.o: /usr/include/i386/_types.h /usr/include/openssl/buffer.h
aws_s3_misc.o: /usr/include/openssl/ossl_typ.h /usr/include/sys/types.h
aws_s3_misc.o: /usr/include/sys/appleapiopts.h /usr/include/machine/types.h
aws_s3_misc.o: /usr/include/i386/types.h /usr/include/machine/endian.h
aws_s3_misc.o: /usr/include/i386/endian.h /usr/include/sys/_endian.h
aws_s3_misc.o: /usr/include/libkern/_OSByteOrder.h
aws_s3_misc.o: /usr/include/libkern/i386/_OSByteOrder.h
aws_s3_misc.o: /usr/include/sys/_structs.h /usr/include/openssl/hmac.h
aws_s3_misc.o: /usr/include/openssl/evp.h /usr/include/openssl/symhacks.h
aws_s3_misc.o: /usr/include/openssl/bio.h /usr/include/stdio.h
aws_s3_misc.o: /usr/include/secure/_stdio.h /usr/include/secure/_common.h
aws_s3_misc.o: /usr/include/stdarg.h /usr/include/openssl/crypto.h
aws_s3_misc.o: /usr/include/stdlib.h /usr/include/Availability.h
aws_s3_misc.o: /usr/include/AvailabilityInternal.h /usr/include/sys/wait.h
aws_s3_misc.o: /usr/include/sys/signal.h /usr/include/machine/signal.h
aws_s3_misc.o: /usr/include/i386/signal.h /usr/include/i386/_structs.h
aws_s3_misc.o: /usr/include/sys/resource.h /usr/include/alloca.h
aws_s3_misc.o: /usr/include/openssl/stack.h /usr/include/openssl/safestack.h
aws_s3_misc.o: /usr/include/openssl/opensslv.h /usr/include/openssl/objects.h
aws_s3_misc.o: /usr/include/openssl/obj_mac.h /usr/include/openssl/asn1.h
aws_s3_misc.o: /usr/include/time.h /usr/include/_structs.h
aws_s3_misc.o: /usr/include/openssl/bn.h
