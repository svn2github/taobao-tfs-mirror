AM_CPPFLAGS=-I$(top_srcdir)/src\
      $(TAIR_CACHE_CPPFLAGS)\
	    -I$(TBLIB_ROOT)/include/tbsys\
            -I$(TBLIB_ROOT)/include/tbnet\
	    $(READLINE_INCLUDE)

AM_LDFLAGS=-lz -lrt -lpthread -ldl $(READLINE_LIB) -luuid

bin_PROGRAMS = parse gene rcstat

LDADD = $(top_builddir)/src/tools/util/libtfstoolsutil.a\
	$(top_builddir)/src/dataserver/libdataserver.a\
	$(top_builddir)/src/message/libtfsmessage.a\
	$(top_builddir)/src/common/libtfscommon.a\
	$(UNIQUE_STORE_LDFLAGS)\
  $(TAIR_CACHE_LDFLAGS) \
	$(TBLIB_ROOT)/lib/libtbnet.a\
	$(TBLIB_ROOT)/lib/libtbsys.a



parse_SOURCES = parse_session_id.cpp
gene_SOURCES = gene_session_id.cpp
rcstat_SOURCES = rcstat.cpp rcstat.h
#test_http_agent_SOURCES = test_http_agent.cpp http_agent.cpp

