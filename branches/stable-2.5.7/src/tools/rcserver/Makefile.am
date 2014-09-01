AM_CPPFLAGS=-I$(top_srcdir)/src\
	    -I$(TBLIB_ROOT)/include/tbsys\
      -I$(TBLIB_ROOT)/include/tbnet\
			-I$(EASY_ROOT)/include/easy

AM_LDFLAGS=-lz -lrt -lpthread -ldl  -luuid -lssl

bin_PROGRAMS = parse gene rcstat

LDADD = $(top_builddir)/src/tools/util/libtfstoolsutil.a\
	$(top_builddir)/src/dataserver/libdataserver.a\
	$(top_builddir)/src/message/libtfsmessage.a\
	$(top_builddir)/src/common/libtfscommon.a\
	$(TBLIB_ROOT)/lib/libtbnet.a\
	$(TBLIB_ROOT)/lib/libtbsys.a\
	$(EASY_ROOT)/lib64/libeasy.a

parse_SOURCES = parse_session_id.cpp
gene_SOURCES = gene_session_id.cpp
rcstat_SOURCES = rcstat.cpp rcstat.h

