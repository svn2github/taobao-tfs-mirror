AM_CPPFLAGS =-I$(top_srcdir)/src\
	     -I$(TBLIB_ROOT)/include/tbsys\
	     -I$(TBLIB_ROOT)/include/tbnet

AM_LDFLAGS=-lrt -lpthread -ldl -ljemalloc

LDADD=$(top_builddir)/src/clientv2/.libs/libtfsclientv2.a\
			$(top_builddir)/src/requester/libtfsrequester.a\
      $(top_builddir)/src/message/libtfsmessage.a\
      $(top_builddir)/src/common/libtfscommon.a\
      $(TBLIB_ROOT)/lib/libtbnet.a\
      $(TBLIB_ROOT)/lib/libtbsys.a

noinst_LIBRARIES = libdataserver.a

DATASERVER_SOURCE_LIST_HEADER=ds_define.h blockfile_format.h super_block_manager.h physical_blockv2.h\
physical_block_manager.h index_handlev2.h data_handlev2.h data_file.h\
logic_blockv2.h logic_block_manager.h block_manager.h lease_managerv2.h gc.h \
client_request_server.h  migrate_manager.h\
dataservice.h data_helper.h galois.h jerasure.h erasure_code.h task_manager.h task.h\
traffic_control.h check_manager.h aop_meta.h aop_manager.h writable_block.h \
writable_block_manager.h sync_manager.h migrate_manager.h

DATASERVER_SOURCE_LIST=ds_define.cpp super_block_manager.cpp physical_blockv2.cpp\
physical_block_manager.cpp index_handlev2.cpp data_handlev2.cpp data_file.cpp \
logic_blockv2.cpp logic_block_manager.cpp block_manager.cpp lease_managerv2.cpp gc.cpp \
client_request_server.cpp  migrate_manager.cpp\
dataservice.cpp data_helper.cpp galois.cpp jerasure.cpp erasure_code.cpp task_manager.cpp task.cpp\
traffic_control.cpp check_manager.cpp aop_meta.cpp aop_manager.cpp writable_block.cpp \
writable_block_manager.cpp sync_manager.cpp

libdataserver_a_SOURCES=$(DATASERVER_SOURCE_LIST) $(DATASERVER_SOURCE_LIST_HEADER)

bin_PROGRAMS = dataserver
dataserver_SOURCES = service.cpp ${libdataserver_a_SOURCES}
