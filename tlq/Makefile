JX = client/jx_test

$(JX): ../dttools/src/jx_test
	cd ../dttools && make
	cp ../dttools/src/jx_test ./client/

all: $(JX)

clean:
	@echo Cleaning TLQ installation.
	cd client && rm -rf .tlq_session.log deposits.log jx_test

install: $(JX)

.PHONY: all clean install

# vim: set noexpandtab tabstop=4:
