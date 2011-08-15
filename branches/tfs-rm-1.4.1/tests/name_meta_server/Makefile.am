AM_CPPFLAGS=-I${top_srcdir}/src/name_meta_server \
            -I$(top_srcdir)/src \
						-I${top_srcdir}/src/common \
						-I$(TBLIB_ROOT)/include/tbsys \
						-I$(TBLIB_ROOT)/include/tbnet

AM_LDFLAGS=-lpthread -lrt -lc -lm -lgtest -luuid `mysql_config --libs`
CXXFLAGS= -g

LDADD=$(top_builddir)/src/name_meta_server/libnamemetaserver.a \
      $(top_builddir)/src/new_client/.libs/libtfsclient.a \
			$(top_builddir)/src/message/libtfsmessage.a \
      $(top_builddir)/src/common/libtfscommon.a \
			$(TBLIB_ROOT)/lib/libtbnet.a \
			$(TBLIB_ROOT)/lib/libtbsys.a 

test: check
.PHONY: test

TESTS=
check_PROGRAMS=

noinst_PROGRAMS=test_meta_info

TESTS+=test_meta_info
check_PROGRAMS+=test_meta_info
test_meta_info_SOURCES=test_meta_info.cpp

