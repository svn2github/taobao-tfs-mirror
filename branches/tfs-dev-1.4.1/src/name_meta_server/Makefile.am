AM_CPPFLAGS=-Wall -I$(top_srcdir)/src\
            -I$(TBLIB_ROOT)/include/tbsys\
            -I$(TBLIB_ROOT)/include/tbnet\
						`mysql_config --include`
 
AM_LDFLAGS=-lz -lrt -lpthread -ldl -luuid `mysql_config --libs_r`

LDADD=$(top_builddir)/src/message/libtfsmessage.a \
      $(top_builddir)/src/common/libtfscommon.a\
      $(TBLIB_ROOT)/lib/libtbnet.a \
      $(TBLIB_ROOT)/lib/libtbsys.a 

noinst_LIBRARIES=libnamemetaserver.a

libnamemetaserver_a_SOURCES=meta_server_service.cpp meta_store_manager.cpp \
			    database_helper.cpp mysql_database_helper.cpp \
                            database_pool.cpp metaserver_define.cpp

bin_PROGRAMS = test test_pool test_service

#metaserver_SOURCES=$(libnamemetaserver_a_SOURCES) main.cpp

test_SOURCES=$(libnamemetaserver_a_SOURCES) test.cpp

test_pool_SOURCES=$(libnamemetaserver_a_SOURCES) test_pool.cpp

test_service_SOURCES=$(libnamemetaserver_a_SOURCES) test_service.cpp
