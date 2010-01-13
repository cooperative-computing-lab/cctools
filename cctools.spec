#
# To build, from a directory containing the Source0 and Patch* files, run:
#  rpmbuild --define "_sourcedir $PWD" --define "_builddir $PWD" --define "_srcrpmdir $PWD" --define "_rpmdir $PWD" --define "_specdir $PWD" --target i686 -ba cctools.spec
#

Name: cctools
Version: 2.6.0
Release: 1%{?dist}

Summary: A collection of software that help users to share resources

License: GPLv2
Group: Development/System
URL: http://www.nd.edu/~ccl/software/
Source0: http://www.nd.edu/~ccl/software/files/cctools-2.6.0-src.tar.gz
Patch0: 0001-Avoid-conditional-test-error-when-globus_flavor-is-e.patch
Patch1: 0002-Detect-Globus-on-Fedora-libraries-in-usr-lib-headers.patch
Patch2: 0003-Detech-fuse-of-Fedora-libfuse-lives-in-lib.patch
Patch3: 0004-Removed-rpath-from-parrot_helper.so-was-hardcoded-to.patch
BuildRoot: %(mktemp -ud %{_tmppath}/%{name}-%{version}-%{release}-XXXXXX)

# Globus
BuildRequires: /usr/include/globus/globus_common.h
BuildRequires: /usr/lib/libglobus_gss_assist.so
BuildRequires: /usr/lib/libglobus_gssapi_gsi.so
BuildRequires: /usr/lib/libglobus_gsi_proxy_core.so
BuildRequires: /usr/lib/libglobus_gsi_credential.so
BuildRequires: /usr/lib/libglobus_gsi_callback.so
BuildRequires: /usr/lib/libglobus_oldgaa.so
BuildRequires: /usr/lib/libglobus_gsi_sysconfig.so
BuildRequires: /usr/lib/libglobus_gsi_cert_utils.so
BuildRequires: /usr/lib/libglobus_openssl.so
BuildRequires: /usr/lib/libglobus_openssl_error.so
BuildRequires: /usr/lib/libglobus_callout.so
BuildRequires: /usr/lib/libglobus_proxy_ssl.so
BuildRequires: /usr/lib/libglobus_common.so
BuildRequires: /usr/lib/libltdl.so
BuildRequires: /usr/lib/globus/include/globus_config.h

# Fuse
BuildRequires: /usr/include/fuse.h
BuildRequires: /lib/libfuse.so

# MySQL
BuildRequires: /usr/include/mysql/mysql.h
BuildRequires: /usr/lib/mysql/libmysqlclient.a
BuildRequires: /usr/lib/mysql/libmystrings.a
BuildRequires: /usr/lib/mysql/libmysys.a

# Other
BuildRequires: /usr/lib/libcom_err.a

%description
The Cooperative Computing Tools (cctools) are a collection of software
that help users to share resources and get along with each other in a
complex, heterogeneous, unreliable computing environment. The cctools
provide reliable, operating-system-like services without requiring
kernel changes or special privileges.


%package devel
Summary: Headers, libraries and docs for cctools
Group: Development/System
Requires: %name = %version-%release

%description devel
Headers, static libraries, and documentation for cctools


%prep
%setup -q -n %{name}-%{version}-src
%patch0 -p1
%patch1 -p1
%patch2 -p1
%patch3 -p1

# Clean up spurious exec bits
find -type f -perm /a+x -not -name configure -not -name "*.sh" -not -name "*.py" -not -name "*.ftsh" -exec chmod a-x {} \;


%build
# This is not an autoconf configure script
./configure
make %{?_smp_mflags}


%install
ROOT=$RPM_BUILD_ROOT/usr

rm -rf $RPM_BUILD_ROOT
make install CCTOOLS_INSTALL_DIR=$RPM_BUILD_ROOT/usr

# Clean up spurious exec bits
find $ROOT/doc -type f -exec chmod a-x {} \;
find $ROOT/include -type f -exec chmod a-x {} \;
find $ROOT/lib -type f -exec chmod a-x {} \;

rm -rf $RPM_BUILD_ROOT/usr/etc/Makefile.config

mkdir -p $RPM_BUILD_ROOT/%_datadir/%{name}
mv $RPM_BUILD_ROOT/usr/doc $RPM_BUILD_ROOT/%_datadir/%{name}


%clean
rm -rf $RPM_BUILD_ROOT


%files
%defattr(-,root,root,-)
%doc COPYING CREDITS
%_bindir/*


%files devel
%defattr(-,root,root,-)
%doc COPYING CREDITS README
%_datadir/%{name}/*
%_includedir/*
%_libdir/libchirp.a
%_libdir/libdttools.a
%_libdir/libftp_lite.a
%_libdir/libparrot_client.a



%changelog
* Sat Jan  9 2010  <matt@redhat> - 2.6.0-1
- Initial package

