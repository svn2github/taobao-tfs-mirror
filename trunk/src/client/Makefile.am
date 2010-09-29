AM_CPPFLAGS=-fPIC -I${top_srcdir}/src\
	-I$(TBLIB_ROOT)/include/tbsys\
	-I$(TBLIB_ROOT)/include/tbnet

AM_LDFLAGS=-lz -lrt -lpthread -ldl

lib_LTLIBRARIES= libtfsclient.la

libtfsclient_la_SOURCES = tfs_file.cpp tfs_session.cpp fsname.cpp tfs_session_pool.cpp tfs_client_api.cpp \
													tfs_file.h tfs_session.h fsname.h tfs_session_pool.h tfs_client_api.h lru.h

libtfsclient_la_LDFLAGS=$(AM_LDFLAGS)

include_HEADERS = tfs_client_api.h
