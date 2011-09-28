
SOURCES=${shell ls man/*.m4}

HTMLFILES = ${SOURCES:%.m4=%.html}
TXTFILES = ${SOURCES:%.m4=%.txt}
PDFFILES = ${SOURCES:%.m4=%.pdf}
MANFILES = ${SOURCES:%.m4=%.man}

all: ${HTMLFILES} ${TXTFILES} ${PDFFILES}

%.html: %.m4
	m4 -DHTML -Imacros $< > $@

%.man: %.m4
	m4 -DMAN -Imacros $< > $@

%.txt: %.man
	nroff -man $< > $@

%.ps: %.man
	groff -man -Tps $< > $@

%.pdf: %.ps
	ps2pdf $< $@

install: all
	mkdir -p $(CCTOOLS_INSTALL_DIR)/doc
	cp *.html $(INSTALL_DIR)/doc
	mkdir -p $(CCTOOLS_INSTALL_DIR)/doc/man
	cp man/*.html $(INSTALL_DIR)/doc/man
	mkdir -p $(CCTOOLS_INSTALL_DIR)/share/man/man1
	cp man/*.txt $(INSTALL_DIR)/share/man/man1

test:

clean:
	rm -f $(HTMLFILES) $(TXTFILES) $(MANFILES) $(PDFFILES) *~
