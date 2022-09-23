include ../../../../config.mk
include $(CCTOOLS_HOME)/rules.mk

# SWIG produces code that causes a lot of warnings, so use -w to turn those off.
# Perl also defines some HAS_* stuff so we need to undefine those too.
LOCAL_CCFLAGS = -w -UHAS_USLEEP -UHAS_ISNAN -UHAS_UTIME -fPIC -Wno-unused-value -Wno-unused-variable $(CCTOOLS_PERL_CCFLAGS) -I $(CCTOOLS_HOME)/dataswarm/src/manager
LOCAL_LINKAGE = $(CCTOOLS_PERL_LDFLAGS) -lz

EXTERNAL_DEPENDENCIES = $(CCTOOLS_HOME)/dataswarm/src/manager/libdataswarm.a $(CCTOOLS_HOME)/dttools/src/libdttools.a
DSPERLSO = dataswarm.$(CCTOOLS_DYNAMIC_SUFFIX)
LIBRARIES = dataswarm.pm $(DSPERLSO)
OBJECTS = ds_wrap.o
TARGETS = $(LIBRARIES)

all: $(TARGETS)

ds_wrap.c dataswarm.pm: dataswarm.i $(EXTERNAL_DEPENDENCIES)
	@echo "SWIG dataswarm.i (perl5)"
	@$(CCTOOLS_SWIG) -o ds_wrap.c -perl5 -exportall -I$(CCTOOLS_HOME)/dttools/src -I$(CCTOOLS_HOME)/dataswarm/src/manager dataswarm.i

$(DSPERLSO): $(OBJECTS) $(EXTERNAL_DEPENDENCIES)

clean:
	rm -f $(OBJECTS) $(TARGETS) *.o ds_wrap.c

test: all

install: all
	mkdir -p $(CCTOOLS_PERL_PATH)/Data_Swarm
	cp $(LIBRARIES)       $(CCTOOLS_PERL_PATH)/
	cp Data_Swarm.pm      $(CCTOOLS_PERL_PATH)
	cp Data_Swarm/Task.pm $(CCTOOLS_PERL_PATH)/Data_Swarm
	mkdir -p $(CCTOOLS_INSTALL_DIR)/doc
	chmod 755 ds_example.pl
	cp ds_example.pl $(CCTOOLS_INSTALL_DIR)/doc/
