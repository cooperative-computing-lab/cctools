include ../../config.mk

SOURCES=${shell find m4 -type f -name '*.m4' -print}

HTMLFILES = ${SOURCES:m4/%.m4=html/%.html}
TXTFILES  = ${SOURCES:m4/%.m4=txt/%.txt}
PDFFILES  = ${SOURCES:m4/%.m4=pdf/%.pdf}
MANFILES  = ${SOURCES:m4/%.m4=man/%.1}
ZIPFILES  = ${MANFILES:man/%.m4=man/%.1.gz}

all: ${CCTOOLS_DOCTARGETS}

apipages:

htmlpages: ${HTMLFILES}

manpages: ${ZIPFILES}

html/%.html: m4/%.m4
	mkdir -p html
	m4 -DHTML ${CCTOOLS_M4_ARGS} $< > $@

man/%.1: m4/%.m4
	mkdir -p man
	m4 -DMAN ${CCTOOLS_M4_ARGS} $< > $@

man/%.gz: man/%.1
	mkdir -p man
	gzip < $< > $@

txt/%.txt: man/%.1
	mkdir -p txt
	nroff -man $< > $@

install: all
	mkdir -p $(CCTOOLS_INSTALL_DIR)/doc
	mkdir -p $(CCTOOLS_INSTALL_DIR)/man
	mkdir -p $(CCTOOLS_INSTALL_DIR)/share/man/man1
	if [ -f man/chirp.1.gz ]; then cp man/*.1.gz $(CCTOOLS_INSTALL_DIR)/share/man/man1; fi
	cp -r html $(CCTOOLS_INSTALL_DIR)/doc/man/

test:

clean:
	rm -rf $(HTMLFILES) $(MANFILES) $(ZIPFILES) *~

.PHONY: all clean install test
