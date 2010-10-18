AM_CPPFLAGS=-I$(top_srcdir)/src\
			-I$(TBLIB_ROOT)/include/tbsys \
			-I$(TBLIB_ROOT)/include/tbnet

AM_LDFLAGS=-lrt -lpthread -ldl

LDADD=$(top_builddir)/src/message/libtfsmessage.a \
	$(top_builddir)/src/common/libtfscommon.a \
	$(TBLIB_ROOT)/lib/libtbnet.a \
	$(TBLIB_ROOT)/lib/libtbsys.a

bin_PROGRAMS = nameserver_ha_monitor

nameserver_ha_monitor_SOURCES = nameserver_ha_monitor.cpp
