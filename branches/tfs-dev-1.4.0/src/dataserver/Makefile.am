AM_CPPFLAGS =-I$(top_srcdir)/src\
	     -I$(TBLIB_ROOT)/include/tbsys\
	     -I$(TBLIB_ROOT)/include/tbnet

AM_LDFLAGS=-lrt -lpthread -ldl

LDADD=$(top_builddir)/src/new_client/.libs/libtfsclient.a\
      $(top_builddir)/src/message/libtfsmessage.a\
      $(top_builddir)/src/common/libtfscommon.a\
      $(TBLIB_ROOT)/lib/libtbnet.a\
      $(TBLIB_ROOT)/lib/libtbsys.a

noinst_LIBRARIES = libdataserver.a
libdataserver_a_SOURCES = visit_stat.cpp version.cpp file_op.cpp mmap_file.cpp mmap_file_op.cpp\
			  superblock_impl.cpp physical_block.cpp bit_map.cpp index_handle.cpp\
			  data_file.cpp cpu_metrics.cpp logic_block.cpp data_handle.cpp blockfile_manager.cpp\
			  data_management.cpp replicate_block.cpp compact_block.cpp sync_backup.cpp\
			  sync_base.cpp requester.cpp file_repair.cpp block_checker.cpp\
			  visit_stat.h version.h file_op.h mmap_file.h mmap_file_op.h\
			  superblock_impl.h physical_block.h bit_map.h index_handle.h\
			  data_file.h cpu_metrics.h logic_block.h data_handle.h blockfile_manager.h\
			  data_management.h replicate_block.h compact_block.h sync_backup.h\
			  sync_base.h requester.h file_repair.h block_checker.h blockfile_format.h\
			  block_status.h dataserver_define.h superblock.h


bin_PROGRAMS = dataserver
dataserver_SOURCES = dataservice.cpp service.cpp dataservice.h ${libdataserver_a_SOURCES}
