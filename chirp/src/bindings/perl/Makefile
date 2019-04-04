include ../../../../config.mk
include $(CCTOOLS_HOME)/rules.mk

PERL_INSTALL_DIR = $(CCTOOLS_INSTALL_DIR)/lib/perl5/site_perl/$(CCTOOLS_PERL_VERSION)

# SWIG produces code that causes a lot of warnings, so use -w to turn those off.
# Perl also defines some HAS_* stuff so we need to undefine those too.
LOCAL_CCFLAGS = -w -UHAS_USLEEP -UHAS_ISNAN -UHAS_UTIME -fPIC -Wno-unused-value -Wno-unused-variable $(CCTOOLS_PERL_CCFLAGS)
LOCAL_LINKAGE = $(CCTOOLS_PERL_LDFLAGS)  $(CCTOOLS_GLOBUS_LDFLAGS) $(CCTOOLS_EXTERNAL_LINKAGE)

EXTERNAL_DEPENDENCIES = $(CCTOOLS_HOME)/chirp/src/libchirp.a $(CCTOOLS_HOME)/dttools/src/libdttools.a
CHIRPPERLSO = CChirp.$(CCTOOLS_DYNAMIC_SUFFIX)
LIBRARIES = CChirp.pm $(CHIRPPERLSO)
OBJECTS = chirp_wrap.o $(CCTOOLS_HOME)/chirp/src/chirp_swig_wrap.o
TARGETS = $(LIBRARIES)

all: $(TARGETS)

chirp_wrap.c CChirp.pm: chirp.i $(EXTERNAL_DEPENDENCIES)
	@echo "SWIG chirp.i (perl5)"
	@$(CCTOOLS_SWIG) -o chirp_wrap.c -perl5 -exportall -I$(CCTOOLS_HOME)/dttools/src -I$(CCTOOLS_HOME)/chirp/src chirp.i

$(CHIRPPERLSO): $(OBJECTS) $(EXTERNAL_DEPENDENCIES)

clean:
	rm -f $(OBJECTS) $(TARGETS) chirp_wrap.c

test: all

install: all
	mkdir -p $(PERL_INSTALL_DIR)
	cp $(LIBRARIES)       $(PERL_INSTALL_DIR)/
	mkdir -p              $(PERL_INSTALL_DIR)/Chirp
	cp Chirp/Client.pm $(PERL_INSTALL_DIR)/Chirp
	cp Chirp/Stat.pm   $(PERL_INSTALL_DIR)/Chirp
	mkdir -p $(CCTOOLS_INSTALL_DIR)/doc
	chmod 755 chirp_perl_example.pl
	cp chirp_perl_example.pl $(CCTOOLS_INSTALL_DIR)/doc/
