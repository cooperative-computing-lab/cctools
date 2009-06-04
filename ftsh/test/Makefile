
include ../../Makefile.config
include ../../Makefile.rules

TESTS=quotes variables loops recurse functions args try forany forall lists expr

all:

test: $(TESTS)

$(TESTS): ../src/ftsh
	@echo "Parsing $@..."
	@../src/ftsh -p $@.ftsh 2>$@.parsed.err > $@.parsed
	@echo "Reparsing $@..."
	@../src/ftsh -p $@.parsed 2>$@.reparsed.err > $@.reparsed
	@echo "Running test $@..."
	@../src/ftsh -l 100 -f $@.log $@.ftsh > $@.out 2> $@.err
	@echo "Checking output of test $@..."
	@diff $@.out $@.expect
	@touch $@
	@echo "Passed."

install:

clean:
	rm -rf *.out *.err *.log *.parsed *.reparsed a g h i $(TESTS)

