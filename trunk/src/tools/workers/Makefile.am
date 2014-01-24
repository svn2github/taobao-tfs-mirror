AM_CPPFLAGS=-I$(top_srcdir)/src\
	    -I$(TBLIB_ROOT)/include/tbsys\
	    -I$(TBLIB_ROOT)/include/tbnet\
			-DUSE_NEW_CLIENT

AM_LDFLAGS=-lrt -lpthread -ldl -luuid -lz

bin_PROGRAMS = get_all_files_in_block save_file_map_between_cluster

LDADD = $(top_builddir)/src/tools/util/libtfstoolsutil.a\
        $(top_builddir)/src/dataserver/libdataserver.a\
	$(top_builddir)/src/clientv2/.libs/libtfsclientv2.a\
	$(top_builddir)/src/requester/libtfsrequester.a\
	$(top_builddir)/src/message/libtfsmessage.a\
	$(top_builddir)/src/common/libtfscommon.a\
	$(UNIQUE_STORE_LDFLAGS)\
  $(TAIR_CACHE_LDFLAGS) \
	$(TBLIB_ROOT)/lib/libtbnet.a\
	$(TBLIB_ROOT)/lib/libtbsys.a\
	-lz

get_all_files_in_block_SOURCES = get_all_files_in_block.cpp
save_file_map_between_cluster_SOURCES = save_file_map_between_cluster.cpp
