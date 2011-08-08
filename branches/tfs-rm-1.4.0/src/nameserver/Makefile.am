#AM_CPPFLAGS=-fprofile-arcs -ftest-coverage -Wall -I$(top_srcdir)/src
AM_CPPFLAGS=-Wall -I$(top_srcdir)/src\
            -I$(TBLIB_ROOT)/include/tbsys\
            -I$(TBLIB_ROOT)/include/tbnet

#AM_LDFLAGS=-lz -lrt -lpthread -ldl -lgcov
AM_LDFLAGS=-lz -lrt -lpthread -ldl

LDADD=$(top_builddir)/src/message/libtfsmessage.a \
      $(top_builddir)/src/common/libtfscommon.a\
      $(TBLIB_ROOT)/lib/libtbnet.a \
      $(TBLIB_ROOT)/lib/libtbsys.a

noinst_LIBRARIES=libnameserver.a

NAMESERVER_SOURCE_LIST_HEADER=block_chunk.h block_collect.h block_id_factory.h\
	client_request_server.h gc.h global_factory.h heart_manager.h layout_manager.h\
	lease_clerk.h nameserver.h ns_define.h oplog.h oplog_sync_manager.h server_collect.h\
	strategy.h

NAMSERVER_SOURCE_LIST=ns_define.cpp nameserver.cpp gc.cpp block_chunk.cpp\
	block_collect.cpp server_collect.cpp strategy.cpp\
	task.cpp global_factory.cpp  lease_clerk.cpp\
	oplog.cpp block_id_factory.cpp oplog_sync_manager.cpp\
	heart_manager.cpp layout_manager.cpp client_request_server.cpp\
	$(NAMESERVER_SOURCE_LIST_HEADER)


libnameserver_a_SOURCES=$(NAMSERVER_SOURCE_LIST)
libnameserver_a_LIBFLAGS=$(AM_LDFLAGS)

bin_PROGRAMS = nameserver

nameserver_SOURCES=$(NAMSERVER_SOURCE_LIST) service.cpp


