AM_CPPFLAGS=-I$(top_srcdir)/src\
	-I$(TBLIB_ROOT)/include/tbsys\
	-I$(TBLIB_ROOT)/include/tbnet

AM_LDFLAGS=-lz -lrt -lpthread -ldl

bin_PROGRAMS = showsyncoplog tfstool performance ssm rmsyncoplog

LDADD = $(top_builddir)/src/client/.libs/libtfsclient.a\
	$(top_builddir)/src/message/libtfsmessage.a\
	$(top_builddir)/src/common/libtfscommon.a\
	$(TBLIB_ROOT)/lib/libtbnet.a\
	$(TBLIB_ROOT)/lib/libtbsys.a 

showsyncoplog_SOURCES = showsyncoplog.cpp

tfstool_SOURCES =  tfstool.cpp

performance_SOURCES = performance.cpp util.cpp

ssm_SOURCES = main.cpp show.cpp show_factory.cpp metacmp.cpp cmp_factory.cpp common.cpp

rmsyncoplog_SOURCES=rmsyncoplog.cpp

