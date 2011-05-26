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

source_list=nameserver.cpp gc.cpp block_chunk.cpp block_collect.cpp server_collect.cpp \
		strategy.cpp task.cpp global_factory.cpp  lease_clerk.cpp oplog.cpp\
		oplog_sync_manager.cpp heart_manager.cpp layout_manager.cpp client_request_server.cpp

libnameserver_a_SOURCES=$(source_list)
libnameserver_a_LDFLAGS=$(AM_LDFLAGS)

bin_PROGRAMS = nameserver

nameserver_SOURCES=$(source_list) service.cpp 


