AM_CPPFLAGS=-I$(top_srcdir)/src\
            -I$(TBLIB_ROOT)/include/tbsys\
            -I$(TBLIB_ROOT)/include/tbnet\
						$(MYSQL_CFLAGS)
						$(READLINE_INCLUDE)

AM_LDFLAGS=-lz -lrt -lpthread -ldl $(READLINE_LIB) -luuid -lsnappy

bin_PROGRAMS = meta_transfer txt2new check_old_new sync_nginx2kv go_tair_for_kv

LDADD =	$(top_builddir)/src/tools/util/libtfstoolsutil.a\
				$(top_builddir)/src/new_client/.libs/libtfsclient.a \
				$(top_builddir)/src/name_meta_server/libnamemetaserver.a \
        $(TAIR_CACHE_LDFLAGS) $(MYSQL_LDFLAGS)\
				$(top_builddir)/src/message/libtfsmessage.a\
        $(top_builddir)/src/common/libtfscommon.a\
        $(TBLIB_ROOT)/lib/libtbnet.a\
        $(TBLIB_ROOT)/lib/libtbsys.a

meta_transfer_SOURCES = trans_meta.cpp
txt2new_SOURCES = txt2newmeta.cpp
check_old_new_SOURCES = check_old_new.cpp
sync_nginx2kv_SOURCES = sync_nginx2kv.cpp
go_tair_for_kv_SOURCES = go_tair_for_kv.cpp

