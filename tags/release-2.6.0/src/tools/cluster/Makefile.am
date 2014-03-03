AM_CPPFLAGS=-I$(top_srcdir)/src\
	    -I$(TBLIB_ROOT)/include/tbsys\
	    -I$(TBLIB_ROOT)/include/tbnet\
      $(READLINE_INCLUDE)

AM_LDFLAGS=-lrt -lpthread -ldl $(READLINE_LIB) -lsnappy

bin_PROGRAMS = transfer split_item_tool block_toolv2
LDADD = $(top_builddir)/src/tools/util/libtfstoolsutil.a\
  $(top_builddir)/src/requester/libtfsrequester.a\
	$(top_builddir)/src/new_client/.libs/libtfsclient.a\
  $(top_builddir)/src/clientv2/.libs/libtfsclientv2.a\
	$(top_builddir)/src/message/libtfsmessage.a\
	$(top_builddir)/src/common/libtfscommon.a\
	$(TBLIB_ROOT)/lib/libtbnet.a\
	$(TBLIB_ROOT)/lib/libtbsys.a

transfer_SOURCES=transfer.cpp transfer.h

split_item_tool_SOURCES=split_item_tool.cpp 

block_toolv2_SOURCES=block_tool_v2.cpp

# calc_ip_addr_SOURCES=calc_ip_addr.cpp
