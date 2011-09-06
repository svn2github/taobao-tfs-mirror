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

metaserver_INCLUDE=database_helper.h  meta_server_service.h\
									mysql_database_helper.h database_pool.h meta_store_manager.h

libnamemetaserver_a_SOURCES=meta_server_service.cpp meta_store_manager.cpp \
			    database_helper.cpp mysql_database_helper.cpp database_pool.cpp \
					mem_helper.cpp meta_cache_helper.cpp meta_cache_info.cpp

bin_PROGRAMS = metaserver test test_pool test_service test_cache_info

metaserver_SOURCES=$(libnamemetaserver_a_SOURCES) main.cpp

test_SOURCES=$(libnamemetaserver_a_SOURCES) test.cpp

test_pool_SOURCES=$(libnamemetaserver_a_SOURCES) test_pool.cpp

test_service_SOURCES=$(libnamemetaserver_a_SOURCES) test_service.cpp

test_cache_info_LDFLAGS=-lgtest
test_cache_info_SOURCES=$(libnamemetaserver_a_SOURCES) test_cache_info.cpp
