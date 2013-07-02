AM_CPPFLAGS=-I${top_srcdir}/src/kv_meta_server \
            -I$(top_srcdir)/src \
						-I${top_srcdir}/src/common \
						-I$(TBLIB_ROOT)/include/tbsys \
						-I$(TBLIB_ROOT)/include/tbnet -DTFS_GTEST $(TAIR_META_CPPFLAGS)

AM_LDFLAGS=-lpthread -lrt -lc -lm -lgtest $(TAIR_META_LDFLAGS)

LDADD=$(top_builddir)/src/kv_meta_server/libkvmetaserver.a \
			$(top_builddir)/src/message/libtfsmessage.a \
      $(top_builddir)/src/common/libtfscommon.a \
      $(TAIR_META_LDFLAGS)\
			$(TBLIB_ROOT)/lib/libtbnet.a \
			$(TBLIB_ROOT)/lib/libtbsys.a

test: check
.PHONY: test

TESTS=
check_PROGRAMS=

noinst_PROGRAMS=test_split_key test_bucket test_object
TESTS+=test_split_key test_bucket
check_PROGRAMS+=test_split_key test_bucket test_object

test_split_key_SOURCES=test_split_key.cpp
test_split_key_LDFLAGS=${AM_LDFLAGS} -static-libgcc -lgtest

test_bucket_SOURCES=test_bucket.cpp test_kvengine.cpp
test_bucket_LDFLASG=${AM_LDFLAGS} -static-libgcc -lgtest

test_object_SOURCES=test_object.cpp test_kvengine.cpp
test_object_LDFLASG=${AM_LDFLAGS} -static-libgcc -lgtest
