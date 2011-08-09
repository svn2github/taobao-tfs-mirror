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
./configure --prefix=%{_prefix} --with-release=yes
make %{?_smp_mflags}

%install
rm -rf $RPM_BUILD_ROOT
make DESTDIR=$RPM_BUILD_ROOT install

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(0755, admin, admin)
%{_prefix}/bin
%{_prefix}/lib
%{_prefix}/conf
%{_prefix}/scripts
%{_prefix}/logs

%files devel
%{_prefix}/include
%{_prefix}/lib/libtfsclient.*
