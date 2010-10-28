AM_CPPFLAGS=-I${top_srcdir}/src \
            -I$(TBLIB_ROOT)/include/tbsys \
            -I$(TBLIB_ROOT)/include/tbnet

AM_LDFLAGS=-lpthread -lrt

LDADD=$(top_builddir)/src/dataserver/libdataserver.a \
      $(top_builddir)/src/client/.libs/libtfsclient.a \
      $(top_builddir)/src/message/libtfsmessage.a \
      $(top_builddir)/src/common/libtfscommon.a\
      $(TBLIB_ROOT)/lib/libtbnet.a \
      $(TBLIB_ROOT)/lib/libtbsys.a

noinst_PROGRAMS=tfs_ioapi_cases

tfs_ioapi_cases_SOURCES=tfs_ioapi_cases.cpp tfs_ioapi_cases.h
tfs_ioapi_cases_LDFLAGS=${AM_LDFLAGS} -static-libgcc -lgtest
