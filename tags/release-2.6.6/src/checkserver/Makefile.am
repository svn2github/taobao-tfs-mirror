AM_CPPFLAGS=-I$(top_srcdir)/src\
            -I$(TBLIB_ROOT)/include/tbsys\
            -I$(TBLIB_ROOT)/include/tbnet\
						-I$(EASY_ROOT)/include/easy

AM_LDFLAGS=-lrt -lpthread -ldl -ljemalloc -lssl

bin_PROGRAMS = checkserver
LDADD = $(top_builddir)/src/message/libtfsmessage.a\
        $(top_builddir)/src/common/libtfscommon.a\
				$(top_builddir)/src/requester/libtfsrequester.a\
        $(TBLIB_ROOT)/lib/libtbnet.a\
        $(TBLIB_ROOT)/lib/libtbsys.a\
				$(EASY_ROOT)/lib64/libeasy.a

checkserver_SOURCES = service.cpp checkserver.cpp check_manager.cpp default_server_helper.cpp checkserver.h check_manager.h default_server_helper.h base_server_helper.h
