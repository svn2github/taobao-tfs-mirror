AM_CPPFLAGS=-I$(top_srcdir)/src\
            -I$(TBLIB_ROOT)/include/tbsys\
            -I$(TBLIB_ROOT)/include/tbnet\
            $(READLINE_INCLUDE)

AM_LDFLAGS=-lrt -lpthread -ldl $(READLINE_LIB)

bin_PROGRAMS = clear_file_system\
               ds_client\
               format_file_system\
               read_super_block

noinst_PROGRAMS = convert_name\
                  reloadconfig\
                  read_index_tool\
                  reverse_name\
                  modify_super_block\
                  replblk\
                  tfsping\
                  view_local_key

LDADD =	$(top_builddir)/src/tools/util/libtfstoolsutil.a\
				$(top_builddir)/src/dataserver/libdataserver.a\
				$(top_builddir)/src/new_client/.libs/libtfsclient.a\
				$(UNIQUE_STORE_LDFLAGS)\
				$(top_builddir)/src/message/libtfsmessage.a\
        $(top_builddir)/src/common/libtfscommon.a\
        $(TBLIB_ROOT)/lib/libtbnet.a\
        $(TBLIB_ROOT)/lib/libtbsys.a

clear_file_system_SOURCES = clear_file_system.cpp

read_super_block_SOURCES = read_super_block.cpp

convert_name_SOURCES = convert_name.cpp

reloadconfig_SOURCES = reloadconfig.cpp

tfsping_SOURCES = tfsping.cpp

ds_client_SOURCES = ds_client.cpp

replblk_SOURCES = replblk.cpp

modify_super_block_SOURCES = modify_super_block.cpp

reverse_name_SOURCES = reverse_name.cpp

format_file_system_SOURCES = format_file_system.cpp

read_index_tool_SOURCES = read_index_tool.cpp

view_local_key_SOURCES = view_local_key.cpp
