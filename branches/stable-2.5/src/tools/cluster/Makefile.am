AM_CPPFLAGS=-I$(top_srcdir)/src\
	    -I$(TBLIB_ROOT)/include/tbsys\
	    -I$(TBLIB_ROOT)/include/tbnet\
      $(READLINE_INCLUDE)

AM_LDFLAGS=-lrt -lpthread -ldl $(READLINE_LIB) -lsnappy

bin_PROGRAMS = calc_ip_addr transfer  split_item_tool
LDADD = $(top_builddir)/src/tools/util/libtfstoolsutil.a\
  $(top_builddir)/src/requester/libtfsrequester.a\
	$(top_builddir)/src/new_client/.libs/libtfsclient.a\
  $(top_builddir)/src/clientv2/.libs/libtfsclientv2.a\
	$(top_builddir)/src/message/libtfsmessage.a\
	$(top_builddir)/src/common/libtfscommon.a\
	$(UNIQUE_STORE_LDFLAGS)\
  $(TAIR_CACHE_LDFLAGS) \
	$(TBLIB_ROOT)/lib/libtbnet.a\
	$(TBLIB_ROOT)/lib/libtbsys.a


sync_by_blk_SOURCES =  sync_by_blk.cpp sync_file_base.cpp sync_file_base.h
sync_by_log_SOURCES =  sync_by_log.cpp sync_file_base.cpp sync_file_base.h
sync_by_file_SOURCES =  sync_by_file.cpp sync_file_base.cpp sync_file_base.h

transfer_SOURCES =  transfer.cpp 
split_item_tool_SOURCES = split_item_tool.cpp 

sync_by_filev2_SOURCES =  sync_by_file_v2.cpp sync_file_base_v2.cpp sync_file_base_v2.h

transfer_by_file_SOURCES = transfer_by_file.cpp sync_file_base.h
block_tool_SOURCES = blocktool.cpp
block_toolv2_SOURCES = block_tool_v2.cpp

calc_ip_addr_SOURCES= calc_ip_addr.cpp
