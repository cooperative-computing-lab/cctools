#
# Rules for building various sorts of files
#

# Internal compiler flags include -MMD -MP, which write the headers that each object includes to a .d file next to it,
# as a rule such as "address.o: address.c address.h". Including those files rebuilds an object when one of its headers
# changes. The .d files exist only after the first build, and the wildcard skips them until then.
-include $(wildcard *.d)

# This file is included before the first target of each Makefile, so make sets .DEFAULT_GOAL to the first rule in the
# .d files, and a plain "make" would build only that object. Clearing it makes the Makefile's first target, usually
# "all", the default goal again.
.DEFAULT_GOAL :=

%.o: %.c
	$(CCTOOLS_CC) -o $@ -c $(CCTOOLS_INTERNAL_CCFLAGS) $(LOCAL_CCFLAGS) $<

%.o: %.cc
	$(CCTOOLS_CXX) -o $@ -c $(CCTOOLS_INTERNAL_CXXFLAGS) $(LOCAL_CXXFLAGS) $<

%.o: %.C
	$(CCTOOLS_CXX) -o $@ -c $(CCTOOLS_INTERNAL_CXXFLAGS) $(LOCAL_CXXFLAGS) $<

%.a:
	$(CCTOOLS_AR) rv $@ $^
	ranlib $@

%: %.o
ifeq ($(CCTOOLS_STATIC),1)
	$(CCTOOLS_LD) -static -g -o $@ $(LOCAL_LINKAGE) $^ $(CCTOOLS_STATIC_LINKAGE)
else
	$(CCTOOLS_LD) -o $@ $(CCTOOLS_INTERNAL_LDFLAGS) $(LOCAL_LDFLAGS) $^ $(LOCAL_LINKAGE) $(CCTOOLS_EXTERNAL_LINKAGE)
endif

%.so:
	$(CCTOOLS_LD) -o $@ -fPIC $(CCTOOLS_DYNAMIC_FLAG) $(CCTOOLS_INTERNAL_LDFLAGS) $(LOCAL_LDFLAGS) $^ $(LOCAL_LINKAGE) $(CCTOOLS_EXTERNAL_LINKAGE)

%.dylib:
	$(CCTOOLS_LD) -o $@ -fPIC $(CCTOOLS_DYNAMIC_FLAG) $(CCTOOLS_INTERNAL_LDFLAGS) $(LOCAL_LDFLAGS) $^ $(LOCAL_LINKAGE) $(CCTOOLS_EXTERNAL_LINKAGE)

# Cancel Make defined implicit rule:
%: %.c
%: %.cc
%: %.C

.PRECIOUS: %.o
