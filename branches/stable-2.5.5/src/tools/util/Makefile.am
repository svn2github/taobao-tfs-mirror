AM_CPPFLAGS=-I$(top_srcdir)/src\
	    -I$(TBLIB_ROOT)/include/tbsys\
	    -I$(TBLIB_ROOT)/include/tbnet\
			-I$(EASY_ROOT)/include/easy\
	    -fpic

AM_LDFLAGS=-lrt -lpthread -ldl -lssl

noinst_LIBRARIES = libtfstoolsutil.a

libtfstoolsutil_a_SOURCES = tool_util.h tool_util.cpp ds_lib.cpp ds_lib.h util.h util.cpp base_worker.h base_worker.cpp
