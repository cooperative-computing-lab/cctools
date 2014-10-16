#! /bin/sh

cd ~
yum -y install curl-devel expat-devel gettext-devel \
  openssl-devel zlib-devel

wget https://www3.nd.edu/~ccl/research/data/hep-case-study/git-master.tar
tar xvf git-master.tar
cd git-master
make clean
make install
cd ~
