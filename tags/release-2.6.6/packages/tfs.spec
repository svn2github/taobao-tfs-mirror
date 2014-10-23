Name: %NAME
Summary: Taobao file system 
Version: %VERSION
Release: 1%{?dist}
Group: Application
URL: http:://yum.corp.alimama.com
Packager: taobao<opensource@taobao.com>
License: GPL
Vendor: TaoBao
Prefix:%{_prefix}
Source:%{NAME}-%{VERSION}.tar.gz
BuildRoot: %{_tmppath}/%{name}-root

BuildRequires: t-csrd-tbnet-devel = 1.0.8
BuildRequires: t_libeasy = 1.0.17
BuildRequires: t_libeasy-devel = 1.0.17
BuildRequires: MySQL-devel-community = 5.1.48 
BuildRequires: tair-devel = 2.3.2.3
BuildRequires: boost-devel = 1.33.1 
BuildRequires: readline-devel
BuildRequires: ncurses-devel
BuildRequires: google-perftools = 1.7
BuildRequires: jemalloc-devel >= 2.2
BuildRequires: snappy >= 1.1.2
BuildRequires: libunwind
BuildRequires: tfs-client-restful
BuildRequires: json-c-devel = 0.11
BuildRequires: json-c = 0.11
BuildRequires: openssl-devel >= 0.9
Requires: jemalloc-devel >= 2.2
Requires: snappy >= 1.1.2
Requires: google-perftools = 1.7
Requires: libunwind
Requires: readline-devel
Requires: ncurses-devel
Requires: tfs-client-restful
Requires: json-c-devel = 0.11
Requires: json-c = 0.11
Requires: openssl >= 0.9
Requires: t_libeasy = 1.0.17

%define __os_install_post %{nil}
%define debug_package %{nil}

%description
TFS is a distributed file system.

%package devel
Summary: tfs c++ client library
Group: Development/Libraries

%description devel
The %name-devel package contains libraries and header
files for developing applications that use the %name package.

%prep
%setup

%build
chmod u+x build.sh
./build.sh init
./configure --prefix=%{_prefix} --without-tcmalloc --enable-uniquestore --enable-taircache --with-tair-root=/opt/csr/tair-2.3 --with-restful-root=/home/admin/tfs/restful/  --with-libeasy-root=/usr
make %{?_smp_mflags}

%install
rm -rf $RPM_BUILD_ROOT
make DESTDIR=$RPM_BUILD_ROOT install

%clean
rm -rf $RPM_BUILD_ROOT

%post
echo %{_prefix}/lib > /etc/ld.so.conf.d/tfs-%{VERSION}.conf
echo /opt/csr/common/lib >> /etc/ld.so.conf.d/tfs-%{VERSION}.conf
echo /usr/local/lib >> /etc/ld.so.conf.d/tfs-%{VERSION}.conf
echo /usr/local/lib64 >> /etc/ld.so.conf.d/tfs-%{VERSION}.conf
/sbin/ldconfig

%post devel
echo %{_prefix}/lib > /etc/ld.so.conf.d/tfs-%{VERSION}.conf
echo /opt/csr/common/lib >> /etc/ld.so.conf.d/tfs-%{VERSION}.conf
echo /usr/local/lib >> /etc/ld.so.conf.d/tfs-%{VERSION}.conf
echo /usr/local/lib64 >> /etc/ld.so.conf.d/tfs-%{VERSION}.conf
/sbin/ldconfig

%postun
rm  -f /etc/ld.so.conf.d/tfs-%{VERSION}.conf

%files
%defattr(0755, admin, admin)
%{_prefix}
%{_prefix}/bin
%{_prefix}/lib
%{_prefix}/conf
%{_prefix}/scripts
%{_prefix}/logs
%{_prefix}/sql

%files devel
%{_prefix}/include
%{_prefix}/lib/libtfsclientv2.*
