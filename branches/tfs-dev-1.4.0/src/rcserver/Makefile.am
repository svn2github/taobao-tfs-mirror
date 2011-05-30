AM_CPPFLAGS=-Wall -I$(top_srcdir)/src\
            -I$(TBLIB_ROOT)/include/tbsys\
            -I$(TBLIB_ROOT)/include/tbnet\
						`mysql_config --include`
 
AM_LDFLAGS=-lz -lrt -lpthread -ldl -luuid `mysql_config --libs`

LDADD=$(top_builddir)/src/message/libtfsmessage.a \
      $(top_builddir)/src/common/libtfscommon.a\
      $(TBLIB_ROOT)/lib/libtbnet.a \
      $(TBLIB_ROOT)/lib/libtbsys.a 

noinst_LIBRARIES=librcserver.a

librcserver_a_SOURCES=database_helper.cpp mysql_database_helper.cpp app_resource.cpp\
                      resource_manager.cpp base_resource.cpp session_util.cpp\
                      session_manager.cpp rc_service.cpp

#bin_PROGRAMS = rcserver
#rcserver_SOURCES=$(librcserver_a_SOURCES) rc_service.cpp
