AM_CPPFLAGS=-I$(top_srcdir)/src\
	    -I$(TBLIB_ROOT)/include/tbsys\
	    -I$(TBLIB_ROOT)/include/tbnet\
            $(READLINE_INCLUDE)

AM_LDFLAGS=-lrt -lpthread -ldl $(READLINE_LIB)

bin_PROGRAMS = showsyncoplog tfstool showssm performance

LDADD = $(top_builddir)/src/client/.libs/libtfsclient.a\
	$(top_builddir)/src/message/libtfsmessage.a\
	$(top_builddir)/src/dataserver/libdataserver.a\
	$(top_builddir)/src/common/libtfscommon.a\
	$(TBLIB_ROOT)/lib/libtbnet.a\
	$(TBLIB_ROOT)/lib/libtbsys.a

showsyncoplog_SOURCES = showsyncoplog.cpp

tfstool_SOURCES =  tfstool.cpp

showssm_SOURCES =  showssm.cpp

performance_SOURCES = performance.cpp util.cpp
