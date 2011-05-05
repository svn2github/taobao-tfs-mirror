AM_CPPFLAGS=-I${top_srcdir}/src/rcserver \
            -I$(top_srcdir)/src \
						-I${top_srcdir}/src/common \
						-I$(TBLIB_ROOT)/include/tbsys \
						-I$(TBLIB_ROOT)/include/tbnet

AM_LDFLAGS=-lpthread -lrt -lc -lm -lgtest 

LDADD=$(top_builddir)/src/dataserver/librcserver.a \
      $(top_builddir)/src/client/.libs/libtfsclient.a \
			$(top_builddir)/src/message/libtfsmessage.a \
      $(top_builddir)/src/common/libtfscommon.a \
			$(TBLIB_ROOT)/lib/libtbnet.a \
			$(TBLIB_ROOT)/lib/libtbsys.a

test: check
.PHONY: test

TESTS=
check_PROGRAMS=

noinst_PROGRAMS=test_app_resource test_base_resource

TESTS+=test_app_resource
check_PROGRAMS+=test_app_resource
test_file_op_SOURCES=test_app_resource.cpp
test_file_op_LDFLAGS=${AM_LDFLAGS} -static-libgcc -lgtest

TESTS+=test_base_resource
check_PROGRAMS+=test_base_resource
test_bit_map_SOURCES=test_base_resource.cpp
test_bit_map_LDFLAGS=${AM_LDFLAGS} -static-libgcc -lgtest

