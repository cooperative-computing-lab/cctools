#!/bin/sh

rpm_version="${1}"
rpm_release="${2}"

#check whether both rpm_version and rpm_release are set
if [ -z "${rpm_version}" -o -z "${rpm_release}" ]; then
	echo "Please specify both rpm_version and rpm_release: ${0} <rpm_version> <rpm_release>"
	exit 1
fi

echo "${rpm_version}"
echo "${rpm_release}"

#check whether rpmdev-setuptree and rpmbuild are available
if [ -z $(which rpmbuild) -o -z $(which rpmdev-setuptree) ]; then
	echo "Please guarantee rpmdevtools is installed on your system, rpmbuild and rpmdev-setuptree are needed to create RPMs!"
	echo "To install rpmdevtools, please run: yum install rpmdevtools"
	exit 1	
fi

#preserve the path of the current working directory
make_dir=$(pwd)

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
	wget "${source}"
	if [ "$?" -ne 0 ]; then
		echo "Failed to download cctools source code tarball from ${source}"
		exit 1
	fi
fi

cd -

#create a template .spec file for ndcctools. 
#Even if the .spec files are recommended to be put under ~/rpmbuild/SPECS, in fact you can put it in other places, just be consistent the location of the .spec file with the rpmbuild command.
echo "${make_dir}"
cd "${make_dir}"/packaging/rpm
temp_file=$(mktemp -p .)
if [ "$?" -ne 0 ]; then
	echo "Fails to create a tmp spec file under ${make_dir}/packaging/rpm directory!"
	exit 1
else
	echo "Create a tmp spec file:  $(pwd)/${temp_file}"
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
date_str=$(date +"%a %b %d %Y")
echo "* ${date_str} Haiyan Meng <hmeng@nd.edu> - ${rpm_version}-${rpm_release}">>"${temp_file}"
echo "- Initial version of the package">>"${temp_file}"

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
	echo "Fails to delete the tmp spec file: $(pwd)/${temp_file}"
	exit 1
else
	echo "Delete the tmp spec file: $(pwd)/${temp_file}"
fi

cd -
