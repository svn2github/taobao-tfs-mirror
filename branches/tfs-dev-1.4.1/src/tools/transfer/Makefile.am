AM_CPPFLAGS=-I$(top_srcdir)/src\
	    -I$(TBLIB_ROOT)/include/tbsys\
	    -I$(TBLIB_ROOT)/include/tbnet\
			-DUSE_NEW_CLIENT

AM_LDFLAGS=-lrt -lpthread -ldl

bin_PROGRAMS = transfer_block split_block_tool compare_crc

LDADD = $(top_builddir)/src/tools/util/libtfstoolsutil.a\
        $(top_builddir)/src/dataserver/libdataserver.a\
	$(top_builddir)/src/new_client/.libs/libtfsclient.a\
	$(top_builddir)/src/message/libtfsmessage.a\
	$(top_builddir)/src/common/libtfscommon.a\
	$(UNIQUE_STORE_LDFLAGS)\
	$(TBLIB_ROOT)/lib/libtbnet.a\
	$(TBLIB_ROOT)/lib/libtbsys.a

transfer_block_SOURCES = transfer_block.cpp block_console.cpp block_console.h 
split_block_tool_SOURCES = split_block_tool.cpp
compare_crc_SOURCES=compare_crc.cpp compare_crc.h
