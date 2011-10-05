include ../Makefile.config

SOURCES=${shell ls man/*.m4}

HTMLFILES = ${SOURCES:%.m4=%.html}
TXTFILES = ${SOURCES:%.m4=%.txt}
PDFFILES = ${SOURCES:%.m4=%.pdf}
MANFILES = ${SOURCES:%.m4=%.1}
ZIPFILES = ${SOURCES:%.m4=%.1.gz}

all: ${HTMLFILES} $(MANFILES) ${ZIPFILES}

%.html: %.m4
	m4 -DHTML -Imacros $< > $@

%.1: %.m4
	m4 -DMAN -Imacros $< > $@

%.txt: %.1
	nroff -man $< > $@

%.ps: %.man
	groff -man -Tps $< > $@

%.pdf: %.ps
	ps2pdf $< $@

%.gz: %
	gzip < $< > $@

install: all
	mkdir -p $(CCTOOLS_INSTALL_DIR)/doc
	cp *.html $(CCTOOLS_INSTALL_DIR)/doc
	mkdir -p $(CCTOOLS_INSTALL_DIR)/doc/man
	cp man/*.html $(CCTOOLS_INSTALL_DIR)/doc/man
	mkdir -p $(CCTOOLS_INSTALL_DIR)/share/man/man1
	cp man/*.1.gz $(CCTOOLS_INSTALL_DIR)/share/man/man1

test:

clean:
	rm -f $(HTMLFILES) $(TXTFILES) $(MANFILES) $(PDFFILES) $(ZIPFILES) *~
