AM_CPPFLAGS= -fprofile-arcs -ftest-coverage\
	    -I$(top_srcdir)/src \
	    -I${top_srcdir}/src/common \
	    -I${top_srcdir}/src/common/mysql_cluster \
	    -I$(TBLIB_ROOT)/include/tbsys \
	    -I$(TBLIB_ROOT)/include/tbnet \
			$(MYSQL_CFLAGS)

AM_LDFLAGS=-lpthread -lrt -lc -lm -lgtest -lgcov
CXXFLAGS= -g

LDADD=$(top_builddir)/src/common/mysql_cluster/libkvengine_mysql.a \
			$(top_builddir)/src/common/libtfscommon.a \
      $(top_builddir)/src/message/libtfsmessage.a \
      $(TBLIB_ROOT)/lib/libtbnet.a \
      $(TBLIB_ROOT)/lib/libtbsys.a

#test: check
#.PHONY: test

noinst_PROGRAMS= test_base_service test_mysql_helper test_http_agent

#noinst_PROGRAMS= test_serialization   test_base_service 
#test_serialization_SOURCES= test_serialization.cpp
#test_serialization_LDFLAGS=${AM_LDFLAGS} -static-libgcc -lgtest

#test_packet_SOURCES= test_packet.cpp new_replicate_block_msg.cpp
#test_packet_LDFLAGS=${AM_LDFLAGS} -static-libgcc -lgtest

#test_packet_streamer_SOURCES = test_packet_streamer.cpp new_replicate_block_msg.cpp
#test_packet_streamer_LDFLAGS=${AM_LDFLAGS} -static-libgcc -lgtest
#
test_base_service_SOURCES=test_base_service.cpp test_base_service_helper.h
test_base_service_LDFLAGS=${AM_LDFLAGS} -static-libgcc -lgtest

test_mysql_helper_SOURCES=test_mysql_helper.cpp
test_mysql_helper_LDFLAGS=${AM_LDFLAGS} -static-libgcc -lgtest  $(MYSQL_LDFLAGS)

test_http_agent_SOURCES=test_http_agent.cpp
test_http_agent_LDFLAGS=${AM_LDFLAGS} -static-libgcc -lgtest  $(MYSQL_LDFLAGS)

#test_base_service_client_SOURCE=test_base_service_client.cpp
#test_base_service_client_LDFLAGS=${AM_LDFLAGS} -static-libgcc -lgtest
