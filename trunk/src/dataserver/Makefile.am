AM_CPPFLAGS =-I$(top_srcdir)/src\
	     -I$(TBLIB_ROOT)/include/tbsys\
	     -I$(TBLIB_ROOT)/include/tbnet

AM_LDFLAGS=-lrt -lpthread -ldl -ljemalloc

LDADD=$(top_builddir)/src/new_client/.libs/libtfsclient.a\
      $(top_builddir)/src/message/libtfsmessage.a\
      $(top_builddir)/src/common/libtfscommon.a\
      $(UNIQUE_STORE_LDFLAGS)\
      $(TAIR_CACHE_LDFLAGS)\
      $(TBLIB_ROOT)/lib/libtbnet.a\
      $(TBLIB_ROOT)/lib/libtbsys.a

noinst_LIBRARIES = libdataserver.a
libdataserver_a_SOURCES = visit_stat.cpp version.cpp mmap_file_op.cpp\
			  superblock_impl.cpp physical_block.cpp bit_map.cpp index_handle.cpp gc.cpp\
			  data_file.cpp cpu_metrics.cpp logic_block.cpp data_handle.cpp blockfile_manager.cpp\
			  data_management.cpp check_block.cpp sync_backup.cpp task_manager.cpp task.cpp\
			  sync_base.cpp requester.cpp file_repair.cpp block_checker.cpp dataservice.cpp\
				galois.cpp jerasure.cpp erasure_code.cpp lease_manager.cpp galois.h jerasure.h erasure_code.h\
				bit_map.h block_checker.h blockfile_format.h blockfile_manager.h block_status.h\
				cpu_metrics.h data_file.h data_handle.h data_management.h task.h\
				dataserver_define.h file_repair.h index_handle.h logic_block.h lease_manager.h\
				mmap_file_op.h physical_block.h check_block.h requester.h gc.h task_manager.h\
				superblock_impl.h sync_backup.h sync_base.h version.h visit_stat.h dataservice.h

bin_PROGRAMS = dataserver
dataserver_SOURCES = service.cpp ${libdataserver_a_SOURCES}
