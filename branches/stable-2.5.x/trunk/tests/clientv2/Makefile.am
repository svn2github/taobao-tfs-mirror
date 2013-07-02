AM_CPPFLAGS=-I${top_srcdir}/src/clientv2 \
            -I$(top_srcdir)/src \
						-I${top_srcdir}/src/common \
						-I${top_srcdir}/src/message \
						-I$(TBLIB_ROOT)/include/tbsys \
						-I$(TBLIB_ROOT)/include/tbnet 

AM_LDFLAGS=-lpthread -lrt -lc -lm -lgtest -static-libgcc

LDADD=$(top_builddir)/src/clientv2/.libs/libtfsclientv2.a \
			$(top_builddir)/src/message/libtfsmessage.a \
      $(top_builddir)/src/common/libtfscommon.a \
      $(TAIR_CACHE_LDFLAGS)\
			$(TBLIB_ROOT)/lib/libtbnet.a \
			$(TBLIB_ROOT)/lib/libtbsys.a

test: check
.PHONY: test

TESTS=
check_PROGRAMS=

noinst_PROGRAMS=test_save_fetch

test_save_fetch_SOURCES=test_save_fetch.cpp
test_save_fetch_LDFLAGS=${AM_LDFLAGS}

