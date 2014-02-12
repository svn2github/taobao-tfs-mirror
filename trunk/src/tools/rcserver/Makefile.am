AM_CPPFLAGS=-I$(top_srcdir)/src\
	    -I$(TBLIB_ROOT)/include/tbsys\
      -I$(TBLIB_ROOT)/include/tbnet

AM_LDFLAGS=-lz -lrt -lpthread -ldl  -luuid

bin_PROGRAMS = parse gene rcstat

<<<<<<< .mine
LDADD = $(top_builddir)/src/message/libtfsmessage.a\
=======
LDADD = $(top_builddir)/src/tools/util/libtfstoolsutil.a\
	$(top_builddir)/src/dataserver/libdataserver.a\
	$(top_builddir)/src/message/libtfsmessage.a\
>>>>>>> .r2790
	$(top_builddir)/src/common/libtfscommon.a\
	$(TBLIB_ROOT)/lib/libtbnet.a\
	$(TBLIB_ROOT)/lib/libtbsys.a

parse_SOURCES = parse_session_id.cpp
gene_SOURCES = gene_session_id.cpp
rcstat_SOURCES = rcstat.cpp rcstat.h

