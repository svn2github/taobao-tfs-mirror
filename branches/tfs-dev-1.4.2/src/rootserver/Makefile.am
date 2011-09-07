#AM_CPPFLAGS=-fprofile-arcs -ftest-coverage -Wall -I$(top_srcdir)/src
AM_CPPFLAGS= -e -Wall -I$(top_srcdir)/src\
            -I$(TBLIB_ROOT)/include/tbsys\
            -I$(TBLIB_ROOT)/include/tbnet

#AM_LDFLAGS=-lz -lrt -lpthread -ldl -lgcov
AM_LDFLAGS=-lz -lrt -lpthread -ldl

LDADD=$(top_builddir)/src/message/libtfsmessage.a \
      $(top_builddir)/src/common/libtfscommon.a\
      $(TBLIB_ROOT)/lib/libtbnet.a \
      $(TBLIB_ROOT)/lib/libtbsys.a

noinst_LIBRARIES=librootserver.a

NAMESERVER_SOURCE_LIST_HEADER=rootserver.h meta_server_manager.h build_tables.h global_factory.h

ROOTSERVER_SOURCE_LIST=rootserver.cpp meta_server_manager.cpp build_table.cpp\
	global_factory.cpp $(NAMESERVER_SOURCE_LIST_HEADER)


librootserver_a_SOURCES=$(ROOTSERVER_SOURCE_LIST)
librootserver_a_LIBFLAGS=$(AM_LDFLAGS)

bin_PROGRAMS = rootserver

rootserver_SOURCES=$(ROOTSERVER_SOURCE_LIST) service.cpp


