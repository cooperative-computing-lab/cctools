#
# Rules for building various sorts of files
#

# Internal compiler flags include -MMD -MP, which write the headers that each object includes to a .d file next to it.
# Including those files rebuilds an object when one of its headers changes. The rules in them must not become the
# default goal, so the goal is cleared and the first target of the Makefile becomes the default again.
-include $(wildcard *.d)
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
