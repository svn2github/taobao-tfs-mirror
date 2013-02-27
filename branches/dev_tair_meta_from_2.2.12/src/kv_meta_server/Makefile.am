AM_CPPFLAGS=-Wall -I$(top_srcdir)/src\
            -I$(TBLIB_ROOT)/include/tbsys\
            -I$(TBLIB_ROOT)/include/tbnet \
						  $(TAIR_META_CPPFLAGS)

AM_LDFLAGS=-lz -lrt -lpthread -ldl -luuid $(TAIR_META_LDFLAGS)

LDADD=$(top_builddir)/src/message/libtfsmessage.a \
      $(top_builddir)/src/common/libtfscommon.a\
      $(TBLIB_ROOT)/lib/libtbnet.a \
      $(TBLIB_ROOT)/lib/libtbsys.a 

noinst_LIBRARIES=libkvmetaserver.a libkvrootserver.a

kvmetaserver_INCLUDE=kvengine_helper.h        kv_meta_server_manager.h  kv_root_server.h    tairengine_helper.h kv_meta_heart_manager.h  kv_meta_service.h         meta_info_helper.h

if WITH_KV_META
tair_kv_source=tairengine_helper.cpp tairengine_helper.h
endif

libkvmetaserver_a_SOURCES=kvengine_helper.cpp kv_meta_service.cpp meta_info_helper.cpp kv_meta_heart_manager.cpp \
													$(tair_kv_source)  $(kvmetaserver_INCLUDE)

KVROOTSERVER_SOURCE_LIST_HEADER=kv_root_server.h kv_meta_server_manager.h
KVROOTSERVER_SOURCE_LIST=kv_root_server.cpp kv_meta_server_manager.cpp $(KVROOTSERVER_SOURCE_LIST_HEADER)

libkvrootserver_a_SOURCES=$(KVROOTSERVER_SOURCE_LIST)
libkvrootserver_a_LIBFLAGS=$(AM_LDFLAGS)

bin_PROGRAMS = kvmetaserver kvrootserver # tool_client 

kvmetaserver_SOURCES=$(libkvmetaserver_a_SOURCES) main.cpp
kvrootserver_SOURCES=$(KVROOTSERVER_SOURCE_LIST) service.cpp
#tool_client_SOURCES=tool_client.cpp
