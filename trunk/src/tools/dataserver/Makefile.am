AM_CPPFLAGS=-I$(top_srcdir)/src\
	-I$(TBLIB_ROOT)/include/tbsys\
	-I$(TBLIB_ROOT)/include/tbnet

AM_LDFLAGS=-lz -lrt -lpthread -ldl

bin_PROGRAMS = clear_file_system \
               ds_client \
               format_file_system

noinst_PROGRAMS = convert_name \
									reloadconfig \
									read_index_tool \
									reverse_name \
									modify_super_block \
									replblk \
									tfsping \
									read_super_block

LDADD = $(top_builddir)/src/client/.libs/libtfsclient.a\
	$(top_builddir)/src/message/libtfsmessage.a\
	$(top_builddir)/src/dataserver/libdataserver.a\
	$(top_builddir)/src/common/libtfscommon.a\
	$(TBLIB_ROOT)/lib/libtbnet.a\
	$(TBLIB_ROOT)/lib/libtbsys.a 

clear_file_system_SOURCES = clear_file_system.cpp

read_super_block_SOURCES = read_super_block.cpp

convert_name_SOURCES = convert_name.cpp

reloadconfig_SOURCES = reloadconfig.cpp

tfsping_SOURCES = tfsping.cpp

ds_client_SOURCES = ds_client.cpp ds_task.cpp ds_lib.cpp ds_util.cpp ds_task.h ds_lib.h ds_util.h

replblk_SOURCES = replblk.cpp

modify_super_block_SOURCES = modify_super_block.cpp

reverse_name_SOURCES = reverse_name.cpp

format_file_system_SOURCES = format_file_system.cpp

read_index_tool_SOURCES = read_index_tool.cpp

