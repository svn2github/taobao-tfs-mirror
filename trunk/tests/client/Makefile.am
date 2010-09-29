AM_CPPFLAGS=-I${top_srcdir}/src/client \
            -I$(top_srcdir)/src \
						-I${top_srcdir}/src/common \
						-I$(TBLIB_ROOT)/include/tbsys \
						-I$(TBLIB_ROOT)/include/tbnet

AM_LDFLAGS=-lpthread -lz -lrt -lc -lm -lgtest 

LDADD=$(top_builddir)/src/dataserver/libdataserver.a \
      $(top_builddir)/src/client/.libs/libtfsclient.a \
			$(top_builddir)/src/message/libtfsmessage.a \
      $(top_builddir)/src/common/libtfscommon.a \
			$(TBLIB_ROOT)/lib/libtbnet.a \
			$(TBLIB_ROOT)/lib/libtbsys.a 

test: check
.PHONY: test

TESTS=
check_PROGRAMS=

noinst_PROGRAMS=test_fsname

TESTS+=test_fsname
check_PROGRAMS+=test_fsname
test_fsname_SOURCES=test_fsname.cpp
test_fsname_LDFLAGS=${AM_LDFLAGS} -static-libgcc -lgtest
