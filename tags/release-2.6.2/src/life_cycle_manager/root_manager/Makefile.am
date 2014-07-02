#AM_CPPFLAGS=-fprofile-arcs -ftest-coverage -Wall -I$(top_srcdir)/src
AM_CPPFLAGS=-Wall -I$(top_srcdir)/src\
            -I$(TBLIB_ROOT)/include/tbsys\
            -I$(TBLIB_ROOT)/include/tbnet\
						  $(TAIR_META_CPPFLAGS) $(MYSQL_CFLAGS)

#AM_LDFLAGS=-lz -lrt -lpthread -ldl -lgcov
AM_LDFLAGS=-lz -lrt -lpthread -ldl 
LDADD=$(top_builddir)/src/message/libtfsmessage.a \
      $(top_builddir)/src/common/libtfscommon.a\
			$(top_builddir)/src/common/mysql_cluster/libkvengine_mysql.a \
			$(TAIR_META_LDFLAGS) \
			$(MYSQL_LDFLAGS)\
      $(TBLIB_ROOT)/lib/libtbnet.a \
      $(TBLIB_ROOT)/lib/libtbsys.a

noinst_LIBRARIES=librootserver.a

ROOTSERVER_SOURCE_LIST_HEADER=exp_root_server.h exp_server_manager.h handle_task_helper.h #global_factory.h exp_server_heart_manager.h 

ROOTSERVER_SOURCE_LIST=exp_root_server.cpp exp_server_manager.cpp handle_task_helper.cpp\
											 $(tair_kv_source) $(ROOTSERVER_SOURCE_LIST_HEADER) #global_factory.cpp exp_server_heart_manager.cpp


librootserver_a_SOURCES=$(ROOTSERVER_SOURCE_LIST)
librootserver_a_LIBFLAGS=$(AM_LDFLAGS)

bin_PROGRAMS = rootserver

rootserver_SOURCES=$(ROOTSERVER_SOURCE_LIST) service.cpp


