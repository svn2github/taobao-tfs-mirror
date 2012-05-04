#AM_CPPFLAGS=-fprofile-arcs -ftest-coverage -Wall -I$(top_srcdir)/src
AM_CPPFLAGS=-I$(top_srcdir)/src\
            -I$(TBLIB_ROOT)/include/tbsys\
            -I$(TBLIB_ROOT)/include/tbnet

#AM_LDFLAGS=-lz -lrt -lpthread -ldl -lgcov
AM_LDFLAGS=-lz -lrt -lpthread -ldl -rdynamic

LDADD=$(top_builddir)/src/message/libtfsmessage.a \
      $(top_builddir)/src/common/libtfscommon.a\
      $(TBLIB_ROOT)/lib/libtbnet.a \
      $(TBLIB_ROOT)/lib/libtbsys.a

noinst_LIBRARIES=libnameserver.a

NAMESERVER_SOURCE_LIST_HEADER=ns_define.h block_collect.h block_manager.h server_collect.h server_manager.h\
	task.h layout_manager.h block_id_factory.h global_factory.h gc.h client_request_server.h\
	oplog_sync_manager.h nameserver.h oplog.h heart_manager.h

NAMSERVER_SOURCE_LIST=block_collect.cpp block_manager.cpp  server_collect.cpp\
	task.cpp task_manager.cpp server_manager.cpp layout_manager.cpp block_id_factory.cpp global_factory.cpp\
	oplog.cpp oplog_sync_manager.cpp client_request_server.cpp gc.cpp\
	ns_define.cpp nameserver.cpp heart_manager.cpp\
	$(NAMESERVER_SOURCE_LIST_HEADER)


libnameserver_a_SOURCES=$(NAMSERVER_SOURCE_LIST)
libnameserver_a_LIBFLAGS=$(AM_LDFLAGS)

bin_PROGRAMS = nameserver

nameserver_SOURCES=$(NAMSERVER_SOURCE_LIST) service.cpp

