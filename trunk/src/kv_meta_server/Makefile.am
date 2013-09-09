AM_CPPFLAGS=-Wall -I$(top_srcdir)/src\
            -I$(TBLIB_ROOT)/include/tbsys\
            -I$(TBLIB_ROOT)/include/tbnet $(TAIR_META_CPPFLAGS) $(MYSQL_CFLAGS)

AM_LDFLAGS=-lz -lrt -lpthread -ldl -luuid $(TAIR_META_LDFLAGS) 

LDADD=$(top_builddir)/src/message/libtfsmessage.a \
      $(top_builddir)/src/common/mysql_cluster/libkvengine_mysql.a \
      $(top_builddir)/src/common/libtfscommon.a $(TAIR_META_LDFLAGS)\
			$(MYSQL_LDFLAGS)\
      $(TBLIB_ROOT)/lib/libtbnet.a \
      $(TBLIB_ROOT)/lib/libtbsys.a 

noinst_LIBRARIES=libkvmetaserver.a libkvrootserver.a

kvmetaserver_INCLUDE= kv_meta_server_manager.h kv_root_server.h kv_meta_heart_manager.h  kv_meta_service.h meta_info_helper.h life_cycle_helper.h


libkvmetaserver_a_SOURCES=kv_meta_service.cpp meta_info_helper.cpp kv_meta_heart_manager.cpp life_cycle_helper.cpp \
													$(tair_kv_source)  $(kvmetaserver_INCLUDE)

KVROOTSERVER_SOURCE_LIST_HEADER=kv_root_server.h kv_meta_server_manager.h life_cycle_helper.h
KVROOTSERVER_SOURCE_LIST=kv_root_server.cpp kv_meta_server_manager.cpp life_cycle_helper.cpp $(KVROOTSERVER_SOURCE_LIST_HEADER)

libkvrootserver_a_SOURCES=$(KVROOTSERVER_SOURCE_LIST)

bin_PROGRAMS = kvmetaserver kvrootserver # tool_client 

kvmetaserver_SOURCES=$(libkvmetaserver_a_SOURCES) main.cpp
kvrootserver_SOURCES=$(KVROOTSERVER_SOURCE_LIST) service.cpp
#tool_client_SOURCES=tool_client.cpp
