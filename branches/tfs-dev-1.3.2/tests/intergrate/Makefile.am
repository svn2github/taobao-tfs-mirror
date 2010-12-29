AM_CPPFLAGS=-I${top_srcdir}/src \
            -I$(TBLIB_ROOT)/include/tbsys \
            -I$(TBLIB_ROOT)/include/tbnet

AM_LDFLAGS=-lpthread -lrt

LDADD=$(top_builddir)/src/new_client/.libs/libtfsclient.a \
      $(top_builddir)/src/message/libtfsmessage.a \
      $(top_builddir)/src/common/libtfscommon.a\
      $(TBLIB_ROOT)/lib/libtbnet.a \
      $(TBLIB_ROOT)/lib/libtbsys.a

noinst_PROGRAMS=tfs_large_file_cases
#noinst_PROGRAMS=tfs_message_size

tfs_large_file_cases_SOURCES=tfs_large_file_cases.cpp tfs_large_file_cases.h
tfs_large_file_cases_LDFLAGS=${AM_LDFLAGS} -static-libgcc -lgtest

#tfs_message_size_SOURCES=tfs_message_size.cpp
#tfs_message_size_LDFLAGS=${AM_LDFLAGS} $(top_builddir)/src/new_client/.libs/libtfsclient.a \
#     $(top_builddir)/src/message/libtfsmessage.a \
#      $(top_builddir)/src/common/libtfscommon.a\
#      $(TBLIB_ROOT)/lib/libtbnet.a \
#      $(TBLIB_ROOT)/lib/libtbsys.a

