AM_CPPFLAGS =-I$(top_srcdir)/src\
	     -I$(TBLIB_ROOT)/include/tbsys\
	     -I$(TBLIB_ROOT)/include/tbnet\
			 -I$(EASY_ROOT)/include/easy

AM_LDFLAGS=-lrt -lpthread -ldl -lssl

LDADD=$(top_builddir)/src/clientv2/.libs/libtfsclientv2.a\
      $(top_builddir)/src/requester/libtfsrequester.a\
      $(top_builddir)/src/message/libtfsmessage.a\
      $(top_builddir)/src/common/libtfscommon.a\
      $(UNIQUE_STORE_LDFLAGS)\
      $(TAIR_CACHE_LDFLAGS)\
      $(TBLIB_ROOT)/lib/libtbnet.a\
      $(TBLIB_ROOT)/lib/libtbsys.a\
			$(EASY_ROOT)/lib64/libeasy.a

noinst_LIBRARIES = libmigrateserver.a

MIGRATESERVER_SOURCE_LIST_HEADER=ms_define.h migrateserver.h migrate_manager.h

MIGRATESERVER_SOURCE_LIST_CPP=ms_define.cpp migrateserver.cpp migrate_manager.cpp

libmigrateserver_a_SOURCES=$(MIGRATESERVER_SOURCE_LIST_CPP) $(MIGRATESERVER_SOURCE_LIST_HEADER)

bin_PROGRAMS = migrateserver

migrateserver_SOURCES = service.cpp $(libmigrateserver_a_SOURCES)
