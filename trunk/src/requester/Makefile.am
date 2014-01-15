AM_CPPFLAGS =-I$(top_srcdir)/src\
	     -I$(TBLIB_ROOT)/include/tbsys\
	     -I$(TBLIB_ROOT)/include/tbnet

AM_LDFLAGS=-lrt -lpthread -ldl

LDADD=$(top_builddir)/src/message/libtfsmessage.a\
      $(top_builddir)/src/common/libtfscommon.a\
      $(UNIQUE_STORE_LDFLAGS)\
      $(TAIR_CACHE_LDFLAGS)\
      $(TBLIB_ROOT)/lib/libtbnet.a\
      $(TBLIB_ROOT)/lib/libtbsys.a

noinst_LIBRARIES = libtfsrequester.a libtfssyncutil.a

libtfsrequester_a_SOURCES=ns_requester.cpp ds_requester.cpp misc_requester.cpp \
												ns_requester.h ds_requester.h misc_requester.h 

libtfssyncutil_a_SOURCES=sync_util.cpp sync_util.h
