AM_CPPFLAGS=-I${top_srcdir}/src/dataserver \
            -I$(top_srcdir)/src \
						-I${top_srcdir}/src/common \
						-I${top_srcdir}/src/message \
						-I$(TBLIB_ROOT)/include/tbsys \
						-I$(TBLIB_ROOT)/include/tbnet 

AM_LDFLAGS=-lpthread -lrt -lc -lm -lgtest -static-libgcc

LDADD=$(top_builddir)/src/dataserver/libdataserver.a \
      $(top_builddir)/src/clientv2/.libs/libtfsclientv2.a \
			$(top_builddir)/src/message/libtfsmessage.a \
      $(top_builddir)/src/common/libtfscommon.a \
      $(TAIR_CACHE_LDFLAGS)\
			$(TBLIB_ROOT)/lib/libtbnet.a \
			$(TBLIB_ROOT)/lib/libtbsys.a

test: check
.PHONY: test

TESTS=
check_PROGRAMS=

noinst_PROGRAMS=test_super_block_manager test_data_file test_physical_block test_physical_block_manager\
	test_index_handlev2 test_logic_blockv2 test_block_manager test_erasure_code test_check_manager

test_super_block_manager_SOURCES=test_super_block_manager.cpp
test_super_block_manager_LDFLAGS=${AM_LDFLAGS}

test_data_file_SOURCES=test_data_file.cpp
test_data_file_LDFLAGS=${AM_LDFLAGS}

test_physical_block_SOURCES=test_physical_block.cpp
test_physical_block_LDFLAGS=${AM_LDFLAGS}

test_physical_block_manager_SOURCES=test_physical_block_manager.cpp
test_physical_block_manager_LDFLAGS=${AM_LDFLAGS}

test_index_handlev2_SOURCES=test_index_handlev2.cpp
test_index_handlev2_LDFLAGS=${AM_LDFLAGS}

test_logic_blockv2_SOURCES=test_logic_blockv2.cpp
test_logic_blockv2_LDFLAGS=${AM_LDFLAGS}

test_block_manager_SOURCES=test_block_manager.cpp
test_block_manager_LDFLAGS=${AM_LDFLAGS}

test_erasure_code_SOURCES=test_erasure_code.cpp
test_erasure_code_LDFLAGS=${AM_LDFLAGS}

test_check_manager_SOURCES=test_check_manager.cpp
test_check_manager_LDFLAGS=${AM_LDFLAGS}

