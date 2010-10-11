AM_CPPFLAGS=-I$(top_srcdir)/src\
            -I$(TBLIB_ROOT)/include/tbsys\
            -I$(TBLIB_ROOT)/include/tbnet
 
AM_LDFLAGS=-lz -lrt -lpthread -ldl 
LDADD=$(top_builddir)/src/message/libtfsmessage.a \
      $(top_builddir)/src/common/libtfscommon.a\
      $(TBLIB_ROOT)/lib/libtbnet.a \
      $(TBLIB_ROOT)/lib/libtbsys.a

bin_PROGRAMS = nameserver
nameserver_SOURCES= block_chunk.cpp layout_manager.cpp compact.cpp\
										heart_manager.cpp lease_clerk.cpp meta_manager.cpp\
										file_system_image.cpp service.cpp oplog.cpp \
										oplog_sync_manager.cpp redundant.cpp replicate.cpp\
										scanner_manager.cpp strategy.cpp nameserver.cpp\
										block_chunk.h layout_manager.h compact.h \
										heart_manager.h lease_clerk.h meta_manager.h \
										file_system_image.h service.h oplog.h \
										oplog_sync_manager.h redundant.h replicate.h \
										scanner_manager.h strategy.h nameserver.h \
										block_collect.h ns_define.h server_collect.h \
										meta_scanner.h proactor_data_pipe.h data_container.h
