Summary:        Cooperative Computing Tools
Group:          Applications/System
License:        GPLv2
URL:            http://ccl.cse.nd.edu/software/
Source0:        http://ccl.cse.nd.edu/software/files/cctools-%{version}-source.tar.gz
BuildRoot:       %{_topdir}/BUILDROOT/

BuildRequires:  gcc
BuildRequires:  gcc-c++
BuildRequires:  make
BuildRequires:  m4

#cctools package dependencies
BuildRequires:  fuse-devel
BuildRequires:  python-devel
BuildRequires:  swig
BuildRequires:  libuuid-devel
BuildRequires:  readline-devel
BuildRequires:  zlib-devel
BuildRequires:  perl
BuildRequires:  perl-ExtUtils-Embed
BuildRequires:  cvmfs-devel

#the dependencies of globus-connect-server are complex, and ignored for now.
#BuildRequires:  globus-connect-server

#required by cvmfs-devel
BuildRequires:  openssl-devel

#required by cvmfs applications
BuildRequires:  freetype

Requires(post): info
Requires(preun): info

%description
The Cooperative Computing Tools (%{name}) contains Parrot,
Chirp, Makeflow, Work Queue, SAND, and other software.

%package devel
Summary: CCTools package development libraries
Group: Applications/System

%description devel
The CCTools package static libraries and header files

%prep
%setup -n cctools-%{version}-source -q

%build
./configure --prefix /usr \
	--with-python-path /usr \
	--with-swig-path /usr \
	--with-readline-path /usr \
	--with-zlib-path /usr \
	--with-perl-path /usr \
	--with-cvmfs-path /usr \
	--with-fuse-path /usr \
	--with-uuid-path /usr
make %{?_smp_mflags}

#the globus dependency is too complex and ignored for now. When the globus dependency is ready, just add `--with-globus-path / \` into the `./configure` command.

%install
rm -rf $RPM_BUILD_ROOT
make CCTOOLS_INSTALL_DIR=%{buildroot}/usr install
rm -rf %{buildroot}/usr/etc
mkdir -p %{buildroot}%{_defaultdocdir}/cctools
mv %{buildroot}/usr/doc/* %{buildroot}%{_defaultdocdir}/cctools
rm -rf %{buildroot}/usr/doc
%ifarch x86_64
mv %{buildroot}/usr/lib %{buildroot}%{_libdir}
%endif

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(-,root,root,-)
%doc %{_defaultdocdir}/cctools/*
#%{_bindir}/*
%{_datadir}/*
%attr(0755,root,root) %{_bindir}/*

%files devel
%defattr(-,root,root,-)
%doc %{_defaultdocdir}/cctools/COPYING
%{_includedir}/cctools/*
%{_libdir}/*.a
%{_libdir}/*.so
%{_libdir}/lib64/*.so
%{_libdir}/python*
%{_libdir}/perl*
%attr(0755,root,root) %{_libdir}/*.so
%attr(0755,root,root) %{_libdir}/lib64/*.so
