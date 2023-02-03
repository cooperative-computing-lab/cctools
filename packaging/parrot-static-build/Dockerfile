# Build as: docker build --tag="cclnd/parrot-run-static-build" --no-cache=true --force-rm .
# Run as:   docker run   -i -t --rm -v /path/to/cctools:/cctools cclnd/parrot-run-static-build
#

FROM centos:7

WORKDIR /

COPY build-static-parrot /bin/

RUN yum update -y && yum install -y make gcc gcc-c++ glibc-static libstdc++-static imake perl python patch bzip2 which findutils tar && yum -y clean all
RUN mkdir -p /opt/vc3/cctools-deps &&\
    mkdir -p /opt/vc3/utils &&\
    cd /opt/vc3/utils &&\
	curl -O http://ccl.cse.nd.edu/software/files/vc3-builder &&\
    chmod 755 ./vc3-builder &&\
    ./vc3-builder --no-run --make-jobs=4 --sys python:2.7=/usr --sys perl:5.16=/usr --require fuse --require e2fsprogs --require openssl --require uuid --require libcvmfs &&\
    rm -rf /opt/vc3/vc3-distfiles

CMD ["/opt/vc3/utils/vc3-builder", "--install", "/opt/vc3/utils/vc3-root", "--sys", "python:2.7=/usr", "--sys", "perl:5.16=/usr", "--require", "fuse", "--require", "e2fsprogs", "--require", "openssl", "--require", "uuid", "--require", "libcvmfs", "--home", "cctools", "--", "/bin/build-static-parrot"]
