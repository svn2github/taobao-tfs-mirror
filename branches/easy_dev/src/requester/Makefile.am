AM_CPPFLAGS =-fPIC -I$(top_srcdir)/src\
	     -I$(TBLIB_ROOT)/include/tbsys\
	     -I$(TBLIB_ROOT)/include/tbnet\
			 -I$(EASY_ROOT)/include/easy

AM_LDFLAGS=-lrt -lpthread -ldl

noinst_LIBRARIES = libtfsrequester.a libtfssyncutil.a

libtfsrequester_a_SOURCES=ns_requester.cpp ds_requester.cpp misc_requester.cpp \
												ns_requester.h ds_requester.h misc_requester.h 

libtfssyncutil_a_SOURCES=sync_util.cpp sync_util.h
