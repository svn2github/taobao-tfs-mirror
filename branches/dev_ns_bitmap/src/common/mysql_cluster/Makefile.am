AM_CPPFLAGS=-Wall -I$(top_srcdir)/src\
            -I$(TBLIB_ROOT)/include/tbsys\
            -I$(TBLIB_ROOT)/include/tbnet \
						$(MYSQL_CFLAGS)

AM_LDFLAGS=-lz -lrt -lpthread -ldl -luuid

AM_LDFLAGS+=$(MYSQL_LDFLAGS)

LDADD=$(top_builddir)/src/message/libtfsmessage.a \
      $(top_builddir)/src/common/libtfscommon.a\
      $(TBLIB_ROOT)/lib/libtbnet.a \
      $(TBLIB_ROOT)/lib/libtbsys.a

noinst_LIBRARIES=libkvengine_mysql.a

kvengine_mysql_INCLUDE= database_pool.h  mysql_database_helper.h  mysql_engine_helper.h

libkvengine_mysql_a_SOURCES=database_pool.cpp  mysql_database_helper.cpp mysql_engine_helper.cpp $(kvengine_mysql_INCLUDE)

