AM_CPPFLAGS =-I$(top_srcdir)/src\
	     -I$(TBLIB_ROOT)/include/tbsys\
	     -I$(TBLIB_ROOT)/include/tbnet

AM_LDFLAGS=-lrt -lpthread -ldl

LDADD=$(top_builddir)/src/clientv2/.libs/libtfsclientv2.a\
      $(top_builddir)/src/requester/libtfsrequester.a\
      $(top_builddir)/src/message/libtfsmessage.a\
      $(top_builddir)/src/common/libtfscommon.a\
      $(UNIQUE_STORE_LDFLAGS)\
      $(TAIR_CACHE_LDFLAGS)\
      $(TBLIB_ROOT)/lib/libtbnet.a\
      $(TBLIB_ROOT)/lib/libtbsys.a

noinst_LIBRARIES = libdataserver.a

DATASERVER_SOURCE_LIST_HEADER=ds_define.h blockfile_format.h super_block_manager.h physical_blockv2.h\
physical_block_manager.h index_handlev2.h data_handlev2.h data_file.h\
logic_blockv2.h logic_block_manager.h block_manager.h lease_managerv2.h gc.h requester.h \
client_request_server.h sync_base.h sync_backup.h data_management.h \
dataservice.h data_helper.h galois.h jerasure.h erasure_code.h task_manager.h task.h\
traffic_control.h check_manager.h op_meta.h op_manager.h writable_block.h \
writable_block_manager.h

DATASERVER_SOURCE_LIST=ds_define.cpp super_block_manager.cpp physical_blockv2.cpp\
physical_block_manager.cpp index_handlev2.cpp data_handlev2.cpp data_file.cpp \
logic_blockv2.cpp logic_block_manager.cpp block_manager.cpp lease_managerv2.cpp gc.cpp requester.cpp \
client_request_server.cpp sync_base.cpp sync_backup.cpp data_management.cpp \
dataservice.cpp data_helper.cpp galois.cpp jerasure.cpp erasure_code.cpp task_manager.cpp task.cpp\
traffic_control.cpp check_manager.cpp op_meta.cpp op_manager.cpp writable_block.cpp \
writable_block_manager.cpp

libdataserver_a_SOURCES=$(DATASERVER_SOURCE_LIST) $(DATASERVER_SOURCE_LIST_HEADER)

bin_PROGRAMS = dataserver
dataserver_SOURCES = service.cpp ${libdataserver_a_SOURCES}
