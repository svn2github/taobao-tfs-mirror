AM_CPPFLAGS=-Wall -I$(top_srcdir)/src\
            -I$(TBLIB_ROOT)/include/tbsys\
            -I$(TBLIB_ROOT)/include/tbnet \
						$(MYSQL_CFLAGS)

AM_LDFLAGS=-lz -lrt -lpthread -ldl -luuid

AM_LDFLAGS+=$(MYSQL_LDFLAGS)

LDADD=$(top_builddir)/src/message/libtfsmessage.a \
      $(top_builddir)/src/common/libtfscommon.a\
      $(TBLIB_ROOT)/lib/libtbnet.a \
      $(TBLIB_ROOT)/lib/libtbsys.a

noinst_LIBRARIES=libmetakvserver.a

metakvserver_INCLUDE=meta_kv_service.h

libmetakvserver_a_SOURCES=meta_kv_service.cpp $(metakvserver_INCLUDE)

bin_PROGRAMS = metakvserver 

metakvserver_SOURCES=$(libmetakvserver_a_SOURCES) main.cpp
