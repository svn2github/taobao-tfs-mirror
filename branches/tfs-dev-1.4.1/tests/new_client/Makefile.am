AM_CPPFLAGS=-I${top_srcdir}/src/new_client \
            -I$(top_srcdir)/src \
	    -I${top_srcdir}/src/common \
	    -I$(TBLIB_ROOT)/include/tbsys \
	    -I$(TBLIB_ROOT)/include/tbnet

AM_LDFLAGS=-lpthread -lrt -lc -lm -lgtest

LDADD=$(top_builddir)/src/new_client/.libs/libtfsclient.a \
      $(top_builddir)/src/message/libtfsmessage.a \
      $(top_builddir)/src/common/libtfscommon.a \
      $(TBLIB_ROOT)/lib/libtbnet.a \
      $(TBLIB_ROOT)/lib/libtbsys.a

test: check
.PHONY: test

TESTS=
check_PROGRAMS=

noinst_PROGRAMS= test_meta_client test_distance
TESTS+=test_meta_client
check_PROGRAMS+=test_meta_client

test_meta_client_SOURCES=test_meta_client.cpp
test_meta_client_LDFLAGS=${AM_LDFLAGS} -static-libgcc -lgtest

TESTS+=test_distance
check_PROGRAMS+=test_distance
test_distance_SOURCES=test_distance.cpp
test_distance_LDFLAGS=${AM_LDFLAGS} -static-libgcc -lgtest
