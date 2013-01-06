AM_CPPFLAGS=-I${top_srcdir}/src/meta_with_KV \
            -I$(top_srcdir)/src \
						-I${top_srcdir}/src/common \
						-I$(TBLIB_ROOT)/include/tbsys \
						-I$(TBLIB_ROOT)/include/tbnet -DTFS_GTEST $(TAIR_META_CPPFLAGS)

AM_LDFLAGS=-lpthread -lrt -lc -lm -lgtest $(TAIR_META_LDFLAGS)

LDADD=$(top_builddir)/src/meta_with_KV/libmetakvserver.a \
			$(top_builddir)/src/message/libtfsmessage.a \
      $(top_builddir)/src/common/libtfscommon.a \
      $(TAIR_META_LDFLAGS)\
			$(TBLIB_ROOT)/lib/libtbnet.a \
			$(TBLIB_ROOT)/lib/libtbsys.a

test: check
.PHONY: test

TESTS=
check_PROGRAMS=

noinst_PROGRAMS=test_split_key 
TESTS+=test_split_key
check_PROGRAMS+=test_split_key
test_split_key_SOURCES=test_split_key.cpp
test_split_key_LDFLAGS=${AM_LDFLAGS} -static-libgcc -lgtest

