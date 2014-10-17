AM_CPPFLAGS=-I${top_srcdir}/src \
						-I$(TBLIB_ROOT)/include/tbsys \
						-I$(TBLIB_ROOT)/include/tbnet \
						$(TAIR_CACHE_CPPFLAGS)

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

noinst_PROGRAMS=test_save_fetch test_block_cache

test_save_fetch_SOURCES=test_save_fetch.cpp
test_save_fetch_LDFLAGS=${AM_LDFLAGS}

test_block_cache_SOURCES=test_block_cache.cpp
test_block_cache_LDFLAGS=${AM_LDFLAGS}

