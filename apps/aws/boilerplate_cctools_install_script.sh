#!/bin/bash
apt-get update
apt-get install build-essential automake autoconf zlibc zlib1g-dev -y
cd /home/ubuntu
wget ccl.cse.nd.edu/software/files/cctools-VERSION_NUMBER-SRC_STRING.tar.gz
chmod 755 cctools-VERSION_NUMBER-SRC_STRING.tar.gz
tar -zxvf cctools-VERSION_NUMBER-SRC_STRING.tar.gz
cd cctools-VERSION_NUMBER-SRC_STRING
./configure
make
make install
cp -r /cctools /home/ubuntu/cctools
rm -r /cctools
export PATH=/home/ubuntu/cctools/bin:$PATH
echo 'PATH=/home/ubuntu/cctools/bin:$PATH' >> /home/ubuntu/.bashrc
touch /home/ubuntu/IM_DONE
sleep WAIT_THRICE
rm /home/ubuntu/IM_DONE
