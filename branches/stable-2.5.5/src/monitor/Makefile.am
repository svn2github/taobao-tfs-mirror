AM_CPPFLAGS=-I$(top_srcdir)/src\
			-I$(TBLIB_ROOT)/include/tbsys \
			-I$(TBLIB_ROOT)/include/tbnet \
			-I$(EASY_ROOT)/include/easy

AM_LDFLAGS=-lrt -lpthread -ldl -lssl

LDADD=$(top_builddir)/src/message/libtfsmessage.a \
	$(top_builddir)/src/common/libtfscommon.a \
	$(TBLIB_ROOT)/lib/libtbnet.a \
	$(TBLIB_ROOT)/lib/libtbsys.a \
	$(EASY_ROOT)/lib64/libeasy.a

bin_PROGRAMS = ha_monitor ns_ping

ha_monitor_SOURCES = ha_monitor.cpp

ns_ping_SOURCES = ns_ping.cpp
