include ../Makefile.config
include ../Makefile.rules

all install clean:
	for p in doc sandtools alignment filtering formatting ; do cd $$p && $(MAKE) $@ && cd .. ; done

test: 
	cd example && ./test_example.sh && ./test_blank_task.sh && cd ..
