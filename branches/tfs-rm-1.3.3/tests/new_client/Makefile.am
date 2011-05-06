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

noinst_PROGRAMS=test_local_key test_gc_worker

TESTS+=test_local_key test_gc_worker test_lru
check_PROGRAMS+=test_local_key test_gc_worker test_lru

test_local_key_SOURCES=test_local_key.cpp
test_gc_worker_SOURCES=test_gc_worker.cpp
test_lru_SOURCES=test_lru.cpp
