AM_CPPFLAGS=-I$(top_srcdir)/src\
            -I$(TBLIB_ROOT)/include/tbsys\
            -I$(TBLIB_ROOT)/include/tbnet\
						$(MYSQL_CFLAGS)
						$(READLINE_INCLUDE)

AM_LDFLAGS=-lz -lrt -lpthread -ldl $(READLINE_LIB) -luuid $(MYSQL_LDFLAGS)

bin_PROGRAMS =  dump2mysql press_select meta_transfer txt2new txt2new_V2 check_old_new check_old_new_V2 sync_nginx2kv go_tair_for_kv  # del_from_tair put_bucket_to_tair

LDADD =	$(top_builddir)/src/tools/util/libtfstoolsutil.a\
				$(top_builddir)/src/new_client/.libs/libtfsclient.a \
				$(top_builddir)/src/common/mysql_cluster/libkvengine_mysql.a \
				$(top_builddir)/src/name_meta_server/libnamemetaserver.a \
        $(TAIR_CACHE_LDFLAGS) $(MYSQL_LDFLAGS)\
				$(top_builddir)/src/message/libtfsmessage.a\
        $(top_builddir)/src/common/libtfscommon.a\
        $(TBLIB_ROOT)/lib/libtbnet.a\
        $(TBLIB_ROOT)/lib/libtbsys.a

press_select_SOURCES = press_select.cpp
dump2mysql_SOURCES = dump2mysql.cpp
meta_transfer_SOURCES = trans_meta.cpp
txt2new_SOURCES = txt2newmeta.cpp
txt2new_V2_SOURCES = txt2newmeta_V2.cpp
check_old_new_SOURCES = check_old_new.cpp
check_old_new_V2_SOURCES = check_old_new_V2.cpp
sync_nginx2kv_SOURCES = sync_nginx2kv.cpp
go_tair_for_kv_SOURCES = go_tair_for_kv.cpp
#del_from_tair_SOURCES = del_from_tair.cpp
#put_bucket_to_tair_SOURCES = put_bucket_to_tair.cpp
