AM_CPPFLAGS=-Wall -I$(top_srcdir)/src\
            -I$(TBLIB_ROOT)/include/tbsys\
            -I$(TBLIB_ROOT)/include/tbnet \
						  $(TAIR_META_CPPFLAGS) $(MYSQL_CFLAGS)\
						-I$(TREST_ROOT)/include

AM_LDFLAGS=-lz -lrt -lpthread -ldl -luuid -ljson -lcurl -lidn $(TAIR_META_LDFLAGS) $(MYSQL_LDFLAGS)

LDADD=$(top_builddir)/src/message/libtfsmessage.a \
      $(top_builddir)/src/common/libtfscommon.a\
			$(top_builddir)/src/common/mysql_cluster/libkvengine_mysql.a \
			$(TAIR_META_LDFLAGS) \
      $(TBLIB_ROOT)/lib/libtbnet.a \
      $(TBLIB_ROOT)/lib/libtbsys.a \
      $(TREST_ROOT)/lib/libtfs_cpp_restful_client.a
			
noinst_LIBRARIES=libexpireserver.a

expireserver_INCLUDE=expire_heart_manager.h  expire_service.h  clean_task_helper.h


libexpireserver_a_SOURCES= expire_heart_manager.cpp  expire_service.cpp  clean_task_helper.cpp \
													$(tair_kv_source)  $(expireserver_INCLUDE)


bin_PROGRAMS = expireserver 

expireserver_SOURCES=$(libexpireserver_a_SOURCES) main.cpp
#clean_test_SOURCES=clean_test.cpp


