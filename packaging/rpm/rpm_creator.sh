#!/bin/sh

#Parrot inside the old cctools versions like 4.4.1 and 4.4.2 are buggy.
rpm_version="${1}"
rpm_release="${2}"

#check whether both rpm_version and rpm_release are set
if [ -z "${rpm_version}" -o -z "${rpm_release}" ]; then
	echo "Please specify both rpm_version and rpm_release: ${0} <rpm_version> <rpm_release>"
	exit 1
fi

type which>/dev/null 2>&1
if [ "$?" -ne 0 ]; then
	echo "Please guarantee which is installed on your system!"
	echo "To install which, please run: yum install which"
	exit 1
fi

#check whether rpmdev-setuptree and rpmbuild are available
if [ -z "$(which rpmbuild)" -o -z "$(which rpmdev-setuptree)" ]; then
	echo "Please guarantee rpmdevtools is installed on your system, rpmbuild and rpmdev-setuptree are needed to create RPMs!"
	echo "To install rpmdevtools, please run: yum install rpmdevtools"
	exit 1
fi

#preserve the path of the current working directory
make_dir="$(pwd)"

#set up an RPM build directory in your ~/rpmbuild directory. This command can be executed multiple times without scratching your current ~/rpmbuild directory.
cd ~
rpmdev-setuptree
if [ "$?" -ne 0 ]; then
	echo "rpmdev-setuptree fails to create RPM build directory in your ~/rpmbuild directory!"
	exit 1
fi

#put the source code compressed tarball of cctools into the ~/rpmbuild/SOURCES dirctory:
cd ~/rpmbuild/SOURCES
if [ ! -e cctools-"${rpm_version}"-source.tar.gz ]; then
	source=http://ccl.cse.nd.edu/software/files/cctools-"${rpm_version}"-source.tar.gz
	#first check whether wget is available.
	if [ -z "$(which wget)" ]; then
		echo "Please guarantee wget is installed on your system!"
		echo "To install wget, please run: yum install wget"
		exit 1
	fi
	wget "${source}"
	if [ "$?" -ne 0 ]; then
		echo "Failed to download cctools source code tarball from ${source}"
		exit 1
	fi
fi

cd -

#create a template .spec file for ndcctools.
#Even if the .spec files are recommended to be put under ~/rpmbuild/SPECS, in fact you can put it in other places, just be consistent the location of the .spec file with the rpmbuild command.
cd "${make_dir}"/packaging/rpm
temp_file="$(mktemp -p .)"
if [ "$?" -ne 0 ]; then
	echo "Fails to create a tmp spec file under ${make_dir}/packaging/rpm directory!"
	exit 1
else
	echo "Create a tmp spec file:  "$(pwd)"/${temp_file}"
fi

#set the attributes at the beginning of the spec file
echo "Name: ndcctools">>"${temp_file}"
echo "Version: ${rpm_version}">>"${temp_file}"
echo "Release: ${rpm_release}%{?dist}">>"${temp_file}"

#ndcctools_template.spec contains all the neutral information which should be applied to any rpm version and release.
cat ndcctools_template.spec>>"${temp_file}"

#set the attributes at the end of the spec file
echo "%changelog">>"${temp_file}"
#the name and email addr here should be modified to be more ccl-style.
date_str="$(date +"%a %b %d %Y")"
echo "* ${date_str} Haiyan Meng <hmeng@nd.edu> - ${rpm_version}-${rpm_release}">>"${temp_file}"
echo "- Initial version of the package">>"${temp_file}"

#check whether tar is available
if [ -z "$(which tar)" ]; then
	echo "Please guarantee tar is installed on your system!"
	echo "To install tar, please run: yum install tar"
	exit 1
fi

#check whether gcc is available
if [ -z "$(which gcc)" ]; then
	echo "Please guarantee gcc is installed on your system!"
	echo "To install gcc, please run: yum install gcc"
	exit 1
fi

#check whether gcc-c++ is available
if [ -z "$(rpm -qa gcc-c++)" ]; then
	echo "Please guarantee gcc-c++ is installed on your system!"
	echo "To install gcc-c++, please run: yum install gcc-c++"
	exit 1
fi

#check whether make is available
if [ -z "$(which make)" ]; then
	echo "Please guarantee make is installed on your system!"
	echo "To install make, please run: yum install make"
	exit 1
fi

#check whether m4 is available
if [ -z "$(which m4)" ]; then
	echo "Please guarantee m4 is installed on your system!"
	echo "To install m4, please run: yum install m4"
	exit 1
fi

#check whether fuse-devel is available
if [ -z "$(rpm -qa fuse-devel)" ]; then
	echo "Please guarantee fuse-devel is installed on your system!"
	echo "To install fuse-devel, please run: yum install fuse-devel"
	exit 1
fi

#check whether grep is available
if [ -z "$(which grep)" ]; then
	echo "Please guarantee grep is installed on your system!"
	echo "To install grep, please run: yum install grep"
	exit 1
fi

libfuse_path="$(rpm -ql fuse-devel | grep libfuse.so)"
libfuse_dir="$(printf "%s" "$libfuse_path" | tail -c +0 | head -c 4)"
echo ${libfuse_dir}
if [ "${libfuse_dir}" != "/usr" -a ! -e "/usr${libfuse_path}" ]; then
	echo "Please put a copy of ${libfuse_path} into /usr${libfuse_path}"
	exit 1
fi

#check whether python-devel is available
if [ -z "$(rpm -qa python-devel)" ]; then
	echo "Please guarantee python-devel is installed on your system!"
	echo "To install python-devel, please run: yum install python-devel"
	exit 1
fi

#check whether swig is available
if [ -z "$(rpm -qa swig)" ]; then
	echo "Please guarantee swig is installed on your system!"
	echo "To install swig, please run: yum install swig"
	exit 1
fi

#check whether libuuid-devel is available
if [ -z "$(rpm -qa libuuid-devel)" ]; then
	echo "Please guarantee libuuid-devel is installed on your system!"
	echo "To install libuuid-devel, please run: yum install libuuid-devel"
	exit 1
fi

#check whether readline-devel is available
if [ -z "$(rpm -qa readline-devel)" ]; then
	echo "Please guarantee readline-devel is installed on your system!"
	echo "To install readline-devel, please run: yum install readline-devel"
	exit 1
fi

#check whether zlib-devel is available
if [ -z "$(rpm -qa zlib-devel)" ]; then
	echo "Please guarantee zlib-devel is installed on your system!"
	echo "To install zlib-devel, please run: yum install zlib-devel"
	exit 1
fi

#check whether perl is available
if [ -z "$(rpm -qa perl)" ]; then
	echo "Please guarantee perl is installed on your system!"
	echo "To install perl, please run: yum install perl"
	exit 1
fi

#check whether perl-ExtUtils-Embed is available
if [ -z "$(rpm -qa perl-ExtUtils-Embed)" ]; then
	echo "Please guarantee perl-ExtUtils-Embed is installed on your system!"
	echo "To install perl-ExtUtils-Embed, please run: yum install perl-ExtUtils-Embed"
	exit 1
fi

#check whether openssl-devel is available
if [ -z "$(rpm -qa openssl-devel)" ]; then
	echo "Please guarantee openssl-devel is installed on your system!"
	echo "To install openssl-devel, please run: yum install openssl-devel"
	exit 1
fi

#check whether cvmfs-devel is available
if [ -z "$(rpm -qa cvmfs-devel)" ]; then
	echo "Please guarantee cvmfs-devel is installed on your system!"
	echo "To install cvmfs-devel, first add cernvm.repo into /etc/yum.repo.d by running the following command:"
	echo "	rpm -Uvh http://cvmrepo.web.cern.ch/cvmrepo/yum/cvmfs/EL/6/`uname -i`/cvmfs-release-2-4.el6.noarch.rpm"
	echo "Then you can install cvmfs-devel by running: yum install cvmfs-devel"
	exit 1
fi

#check whether freetype is available
if [ -z "$(rpm -qa freetype)" ]; then
	echo "Please guarantee freetype is installed on your system!"
	echo "To install freetype, please run: yum install freetype"
	exit 1
fi

#build source and binary RPMs from ndcctools.spec:
rpmbuild -ba "${temp_file}"
if [ "$?" -ne 0 ]; then
	echo "Fails to create RPMs and SRPMs for cctools!"
	exit 1
else
	echo "The created RPMs are under ~/rpmbuild/RPMs"
	echo "The created SRPMs are under ~/rpmbuild/SRPMs"
fi

#remove temp_file
rm -f "${temp_file}"
if [ "$?" -ne 0 ]; then
	echo "Fails to delete the tmp spec file: "$(pwd)"/${temp_file}"
	exit 1
else
	echo "Delete the tmp spec file: "$(pwd)"/${temp_file}"
fi

cd -

# vim: set noexpandtab tabstop=4:
