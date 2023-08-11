BWA_VERSION=bwa-0.7.17

all: bwa work_queue_server work_queue_server.py ref.fastq.ann query.fastq.0.gz

work_queue_server:
	ln -s ../../work_queue/src/work_queue_server .

work_queue_server.py:
	ln -s ../../work_queue/src/clients/python3/work_queue_server.py .

bwa: ${BWA_VERSION}
	cd ${BWA_VERSION} && make && cd .. && cp ${BWA_VERSION}/bwa .

${BWA_VERSION}:
	curl -L https://sourceforge.net/projects/bio-bwa/files/${BWA_VERSION}.tar.bz2/download > ${BWA_VERSION}.tar.bz2
	bunzip2 ${BWA_VERSION}.tar.bz2
	tar xvf ${BWA_VERSION}.tar

ref.fastq: fastq_generate.pl
	./fastq_generate.pl 100000 1000 > ref.fastq

query.fastq: fastq_generate.pl
	./fastq_generate.pl 10000000 10 > query.fastq      #5 splits

ref.fastq.ann: ref.fastq bwa
	./bwa index ref.fastq

query.fastq.0: query.fastq create_splits.py
	./create_splits.py

query.fastq.0.gz: query.fastq.0
	gzip -f query.fastq.*

clean:
	rm -rf query.fastq.0.* ref.fastq ref.fastq.* query.fastq ${BWA_VERSION} ${BWA_VERSION}.* bwa work_queue_server.py work_queue_server *.pyc
