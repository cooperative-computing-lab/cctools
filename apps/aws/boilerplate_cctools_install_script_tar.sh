#!/bin/bash
apt-get update
apt-get install build-essential automake autoconf zlibc zlib1g-dev -y
cd /home/ubuntu 
fileTransferred=0
while [ $fileTransferred -ne 1 ]; do
	homeListing=`ls`
	if [ "$homeListing" = "" ]; then
		sleep WAIT_ONCE
	else
		fileTransferred=1
		touch /home/ubuntu/GOT_IT
	fi	
done

chmod 755 $homeListing
tar -zxvf $homeListing

for x in $(ls -d */);
do
	cd "$x"
	break
done

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
rm /home/ubuntu/GOT_IT
