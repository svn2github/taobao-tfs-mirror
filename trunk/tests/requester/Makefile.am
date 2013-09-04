AM_CPPFLAGS=-I${top_srcdir}/src \
						-I$(TBLIB_ROOT)/include/tbsys \
						-I$(TBLIB_ROOT)/include/tbnet \
						$(TAIR_CACHE_CPPFLAGS)

AM_LDFLAGS=-lpthread -lrt -lc -lm -lgtest -static-libgcc -lsnappy

LDADD=$(top_builddir)/src/requester/libtfsrequester.a \
			$(top_builddir)/src/message/libtfsmessage.a \
      $(top_builddir)/src/common/libtfscommon.a \
			$(TBLIB_ROOT)/lib/libtbnet.a \
			$(TBLIB_ROOT)/lib/libtbsys.a

test: check
.PHONY: test

TESTS=
check_PROGRAMS=

noinst_PROGRAMS=test_ns_requester test_ds_requester test_misc_requester

test_ns_requester_SOURCES=test_ns_requester.cpp
test_ns_requester_LDFLAGS=${AM_LDFLAGS}

test_ds_requester_SOURCES=test_ds_requester.cpp
test_ds_requester_LDFLAGS=${AM_LDFLAGS}

test_misc_requester_SOURCES=test_misc_requester.cpp
test_misc_requester_LDFLAGS=${AM_LDFLAGS}

