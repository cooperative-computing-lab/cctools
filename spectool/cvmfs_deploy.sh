#! /bin/sh

#make sure FUSE can be enabled on this host
if [ ! -e /dev/fuse ]; then 
	echo "fuse has not been enabled on this host!"
	mknod -m 666 /dev/fuse c 10 229
	if [ $? == 0 ]; then
		echo "Now fuse has been enabled on this host!"
	else
		echo "Failed to enable fuse on this host!"
		exit 1
	fi
else
	echo "fuse has already been enabled."
fi

cern_repo="/etc/yum.repos.d/cernvm.repo"
cat > "${cern_repo}" <<EOF
[cernvm]
name=CernVM packages
baseurl=https://cvmrepo.web.cern.ch/cvmrepo/yum/cvmfs/EL/\$releasever/\$basearch/
enabled=1
gpgcheck=0
EOF

#install rpms
yum update -y && yum -y install \
    cvmfs cvmfs-init-scripts cvmfs-auto-setup \
    freetype fuse \
    man nano openssh-server openssl098e libXext libXpm

cvmfs_default_local="/etc/cvmfs/default.local"
cat > "${cvmfs_default_local}" << EOF
CVMFS_REPOSITORIES=atlas,atlas-condb,lhcb,sft.cern.ch
#CVMFS_CACHE_BASE=/scratch/var/cache/cvmfs2
CVMFS_QUOTA_LIMIT=2000
#CVMFS_HTTP_PROXY="http://[YOUR-SQUID-CACHE]:3128"
#CVMFS_HTTP_PROXY="http://ca-proxy.cern.ch:3128"
CVMFS_HTTP_PROXY="http://cache01.hep.wisc.edu:3128"
EOF

cvmfs_domain_cern="/etc/cvmfs/domain.d/cern.ch.local"
cat > "${cvmfs_domain_cern}" << EOF
CVMFS_SERVER_URL="http://cernvmfs.gridpp.rl.ac.uk/opt/@org@;http://cvmfs-stratum-one.cern.ch/opt/@org@;http://cvmfs.racf.bnl.gov/opt/@org@"
CVMFS_PUBLIC_KEY=/etc/cvmfs/keys/cern.ch.pub
EOF

mkdir -p \
    /cvmfs/cernvm-prod.cern.ch \
    /cvmfs/sft.cern.ch \
	/cvmfs/atlas.cern.ch \
	/cvmfs/atlas-condb.cern.ch \
	/cvmfs/lhcb.cern.ch

result="$(cat /etc/fstab | grep '/cvmfs/cernvm-prod.cern.ch')"
if [ -z "${result}" ]; then
	echo "cernvm-prod.cern.ch /cvmfs/cernvm-prod.cern.ch cvmfs defaults 0 0" >> /etc/fstab
fi

result="$(cat /etc/fstab | grep '/cvmfs/sft.cern.ch')"
if [ -z "${result}" ]; then
	echo "sft.cern.ch         /cvmfs/sft.cern.ch cvmfs defaults 0 0" >> /etc/fstab
fi

result="$(cat /etc/fstab | grep '/cvmfs/atlas.cern.ch')"
if [ -z "${result}" ]; then
	echo "atlas.cern.ch       /cvmfs/atlas.cern.ch cvmfs defaults 0 0" >> /etc/fstab
fi

result="$(cat /etc/fstab | grep '/cvmfs/atlas-condb.cern.ch')"
if [ -z "${result}" ]; then
	echo "atlas-condb.cern.ch /cvmfs/atlas-condb.cern.ch cvmfs defaults 0 0" >> /etc/fstab
fi

result="$(cat /etc/fstab | grep '/cvmfs/lhcb.cern.ch')"
if [ -z "${result}" ]; then
	echo "lhcb.cern.ch        /cvmfs/lhcb.cern.ch cvmfs defaults 0 0" >> /etc/fstab
fi

chmod a+w /dev/fuse

echo "::: mounting FUSE..."
mount -a
echo "::: mounting FUSE... [done]"

export VO_ATLAS_SW_DIR="/cvmfs/atlas.cern.ch"
export ATLAS_LOCAL_ROOT_BASE="${VO_ATLAS_SW_DIR}/repo/ATLASLocalRootBase"

## setup ATLAS environment
if [ -e "${ATLAS_LOCAL_ROOT_BASE}/user/atlasLocalSetup.sh" ] ; then
   echo "::: sourcing ALRB..."
   . ${ATLAS_LOCAL_ROOT_BASE}/user/atlasLocalSetup.sh
   echo "::: sourcing ALRB... [done]"
fi










