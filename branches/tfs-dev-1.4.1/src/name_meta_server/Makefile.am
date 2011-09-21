AM_CPPFLAGS=-Wall -I$(top_srcdir)/src\
            -I$(TBLIB_ROOT)/include/tbsys\
            -I$(TBLIB_ROOT)/include/tbnet

AM_LDFLAGS=-lz -lrt -lpthread -ldl -luuid

AM_LDFLAGS+=$(MYSQL_LDFLAGS)

LDADD=$(top_builddir)/src/message/libtfsmessage.a \
      $(top_builddir)/src/common/libtfscommon.a\
      $(TBLIB_ROOT)/lib/libtbnet.a \
      $(TBLIB_ROOT)/lib/libtbsys.a

noinst_LIBRARIES=libnamemetaserver.a

metaserver_INCLUDE=database_helper.h mem_helper.h meta_cache_info.h meta_server_service.h mysql_database_helper.h\
									database_pool.h meta_cache_helper.h  meta_hash_helper.h  meta_store_manager.h

libnamemetaserver_a_SOURCES=meta_server_service.cpp meta_store_manager.cpp \
			    database_helper.cpp mysql_database_helper.cpp database_pool.cpp \
					mem_helper.cpp meta_cache_helper.cpp meta_cache_info.cpp $(metaserver_INCLUDE)

bin_PROGRAMS = metaserver

metaserver_SOURCES=$(libnamemetaserver_a_SOURCES) main.cpp
