AM_CPPFLAGS=-I$(top_srcdir)/src\
            -I$(TBLIB_ROOT)/include/tbsys\
            -I$(TBLIB_ROOT)/include/tbnet\
						$(MYSQL_CFLAGS)

AM_LDFLAGS=-lrt -lpthread -ldl $(MYSQL_LDFLAGS)

bin_PROGRAMS = meta_transfer

LDADD =	$(top_builddir)/src/name_meta_server/libnamemetaserver.a\
        $(TAIR_CACHE_LDFLAGS) \
				$(top_builddir)/src/message/libtfsmessage.a\
        $(top_builddir)/src/common/libtfscommon.a\
        $(TBLIB_ROOT)/lib/libtbnet.a\
        $(TBLIB_ROOT)/lib/libtbsys.a

meta_transfer_SOURCES = trans_meta.cpp

