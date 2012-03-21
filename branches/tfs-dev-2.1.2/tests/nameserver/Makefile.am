AM_CPPFLAGS=-fprofile-arcs -ftest-coverage -I${top_srcdir}/src/nameserver \
	    -I$(top_srcdir)/src \
	    -I${top_srcdir}/src/common \
	    -I$(TBLIB_ROOT)/include/tbsys \
	    -I$(TBLIB_ROOT)/include/tbnet

AM_LDFLAGS=-lpthread -lrt -lc -lm -lgtest -lgcov
 
LDADD=$(top_builddir)/src/nameserver/libnameserver.a \
			$(top_builddir)/src/message/libtfsmessage.a\
      $(top_builddir)/src/common/libtfscommon.a \
      $(TBLIB_ROOT)/lib/libtbnet.a \
      $(TBLIB_ROOT)/lib/libtbsys.a

#test: check
#.PHONY: test

noinst_PROGRAMS= test_layout_manager test_block_collect test_server_collect\
		test_block_chunk test_lease test_task test_gc integration test_strategy

#noinst_PROGRAMS= test_block_collect test_server_collect test_block_chunk \
#		 test_strategy
#test_block_chunk_SOURCES=test_block_chunk.cpp
#test_block_chunk_LDFLAGS=${AM_LDFLAGS} -static-libgcc -lgtest

test_block_collect_SOURCES=test_block_collect.cpp
test_block_collect_LDFLAGS=${AM_LDFLAGS} -static-libgcc -lgtest

test_server_collect_SOURCES=test_server_collect.cpp
test_server_collect_LDFLAGS=${AM_LDFLAGS} -static-libgcc -lgtest

test_block_chunk_SOURCES=test_block_chunk.cpp
test_block_chunk_LDFLAGS=${AM_LDFLAGS} -static-libgcc -lgtest

test_strategy_SOURCES=test_strategy.cpp
test_strategy_LDFLAGS=${AM_LDFLAGS} -static-libgcc -lgtest

test_layout_manager_SOURCES=test_layout_manager.cpp
test_layout_manager_LDFLAGS=${AM_LDFLAGS} -static-libgcc -lgtest

test_task_SOURCES= test_task.cpp
test_task_LDFLAGS=${AM_LDFLAGS} -static-libgcc -lgtest

test_lease_SOURCES= test_lease.cpp
test_lease_LDFLAGS=${AM_LDFLAGS} -static-libgcc -lgtest

test_gc_SOURCES= test_gc.cpp
test_gc_LDFLAGS=${AM_LDFLAGS} -static-libgcc -lgtest

integration_SOURCES= integration.cpp integration_instance.cpp integration_case.cpp
integration_LDFLAGS=${AM_LDFLAGS}
