AM_CPPFLAGS=-I$(top_srcdir)/src\
            -I$(TBLIB_ROOT)/include/tbsys\
            -I$(TBLIB_ROOT)/include/tbnet\
						-I$(EASY_ROOT)/include/easy\
            $(READLINE_INCLUDE)

AM_LDFLAGS=-lrt -lpthread -ldl -lssl $(READLINE_LIB)

bin_PROGRAMS = clear_file_system\
               ds_client\
               format_file_system\
               read_super_block\
							 read_index_tool\
							 convert_name\
               reverse_name\
							 check_down_disk_data\
               check_super_block \
							 check_family \
               view_local_key \
							 safe_remove_block

LDADD =	$(top_builddir)/src/tools/util/libtfstoolsutil.a\
				$(top_builddir)/src/dataserver/libdataserver.a\
				$(top_builddir)/src/clientv2/.libs/libtfsclientv2.a\
				$(top_builddir)/src/requester/libtfsrequester.a\
				$(UNIQUE_STORE_LDFLAGS)\
        $(TAIR_CACHE_LDFLAGS) \
				$(top_builddir)/src/message/libtfsmessage.a\
        $(top_builddir)/src/common/libtfscommon.a\
        $(TBLIB_ROOT)/lib/libtbnet.a\
        $(TBLIB_ROOT)/lib/libtbsys.a\
				$(EASY_ROOT)/lib64/libeasy.a

clear_file_system_SOURCES = clear_file_system.cpp

read_super_block_SOURCES = read_super_block.cpp

check_super_block_SOURCES = check_super_block.cpp

convert_name_SOURCES = convert_name.cpp

ds_client_SOURCES = ds_client.cpp

reverse_name_SOURCES = reverse_name.cpp

format_file_system_SOURCES = format_file_system.cpp

read_index_tool_SOURCES = read_index_tool.cpp

check_down_disk_data_SOURCES = check_down_disk_data.cpp

safe_remove_block_SOURCES = safe_remove_block.cpp

check_family_SOURCES = check_family.cpp

#recover_disk_data_to_clusterv2_SOURCES = recover_disk_data_to_cluster_v2.cpp

#recover_disk_data_to_cluster_SOURCES = recover_disk_data_to_cluster.cpp

#recover_sync_file_queue_SOURCES = recover_sync_file_queue.cpp

#gen_block_prefix_SOURCES = gen_block_prefix.cpp

#reload_config_SOURCES = reload_config.cpp

#tfsping_SOURCES = tfsping.cpp

#modify_super_block_SOURCES = modify_super_block.cpp

#synctool_SOURCES = sync_tool.cpp # for test syncserver

view_local_key_SOURCES = view_local_key.cpp

#read_block_prefix_SOURCES = read_block_prefix.cpp

#verify_block_to_dataserver_SOURCES = verify_block_to_dataserver.cpp
