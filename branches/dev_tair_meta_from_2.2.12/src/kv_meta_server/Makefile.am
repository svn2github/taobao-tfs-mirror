AM_CPPFLAGS=-Wall -I$(top_srcdir)/src\
            -I$(TBLIB_ROOT)/include/tbsys\
            -I$(TBLIB_ROOT)/include/tbnet \
						  $(TAIR_META_CPPFLAGS)

AM_LDFLAGS=-lz -lrt -lpthread -ldl -luuid $(TAIR_META_LDFLAGS)

LDADD=$(top_builddir)/src/message/libtfsmessage.a \
      $(top_builddir)/src/common/libtfscommon.a\
      $(TBLIB_ROOT)/lib/libtbnet.a \
      $(TBLIB_ROOT)/lib/libtbsys.a \

noinst_LIBRARIES=libmetakvserver.a

metakvserver_INCLUDE=kv_meta_service.h

if WITH_KV_META
tair_kv_source=tairengine_helper.cpp tairengine_helper.h
endif

libmetakvserver_a_SOURCES=kvengine_helper.cpp kv_meta_service.cpp meta_info_helper.cpp \
													$(tair_kv_source)  $(metakvserver_INCLUDE)

bin_PROGRAMS = kvmetaserver tool_client 

kvmetaserver_SOURCES=$(libmetakvserver_a_SOURCES) main.cpp
tool_client_SOURCES=tool_client.cpp
