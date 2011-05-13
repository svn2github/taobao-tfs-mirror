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
#librcserver_a_SOURCES=resource_manager.cpp mysql_conn.cpp update_resource.cpp \
#                      app_resource.cpp base_resource.cpp session_manager.cpp\
#                      resource_manager.h uuid.h resource.h mysql_conn.h \
#                      update_resource.h base_resource.h app_resource.h session_manager.h

librcserver_a_SOURCES=session_manager.cpp uuid.cpp
#librcserver_a_SOURCES=database_helper.cpp mysql_database_helper.cpp
											
#bin_PROGRAMS = rcserver
#rcserver_SOURCES=$(librcserver_a_SOURCES) service.cpp 
