AM_CPPFLAGS=-I$(top_srcdir)/src\
	    -I$(TBLIB_ROOT)/include/tbsys\
	    -I$(TBLIB_ROOT)/include/tbnet\
			-I$(EASY_ROOT)/include/easy\
            $(READLINE_INCLUDE)

AM_LDFLAGS=-lrt -lpthread -ldl -lssl $(READLINE_LIB)

bin_PROGRAMS = #adminservertool

LDADD = $(top_builddir)/src/tools/util/libtfstoolsutil.a\
        $(top_builddir)/src/message/libtfsmessage.a\
	$(top_builddir)/src/common/libtfscommon.a\
	$(TBLIB_ROOT)/lib/libtbnet.a\
	$(TBLIB_ROOT)/lib/libtbsys.a\
	$(EASY_ROOT)/lib64/libeasy.a

adminservertool_SOURCES =  adminservertool.cpp
