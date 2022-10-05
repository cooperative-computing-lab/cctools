all: client

client: ../dttools/src/jx_test
	cp ../dttools/src/jx_test client
	
clean:
	@echo Cleaning TLQ installation.
	cd client && rm -rf .tlq_session.log deposits.log jx_test

install:

.PHONY: all clean install

# vim: set noexpandtab tabstop=4:
