AM_CPPFLAGS= -fprofile-arcs -ftest-coverage\
	    -I$(top_srcdir)/src \
	    -I${top_srcdir}/src/common \
	    -I$(TBLIB_ROOT)/include/tbsys \
	    -I$(TBLIB_ROOT)/include/tbnet

AM_LDFLAGS=-lpthread -lrt -lc -lm -lgtest -lgcov

LDADD=$(top_builddir)/src/common/libtfscommon.a\
      $(top_builddir)/src/message/libtfsmessage.a\
      $(TBLIB_ROOT)/lib/libtbnet.a \
      $(TBLIB_ROOT)/lib/libtbsys.a

#test: check
#.PHONY: test

noinst_PROGRAMS= test_serialization  test_packet test_packet_streamer
test_serialization_SOURCES= test_serialization.cpp
test_serialization_LDFLAGS=${AM_LDFLAGS} -static-libgcc -lgtest

test_packet_SOURCES= test_packet.cpp new_replicate_block_msg.cpp
test_packet_LDFLAGS=${AM_LDFLAGS} -static-libgcc -lgtest

test_packet_streamer_SOURCES = test_packet_streamer.cpp new_replicate_block_msg.cpp
test_packet_streamer_LDFLAGS=${AM_LDFLAGS} -static-libgcc -lgtest
