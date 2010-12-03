AM_CPPFLAGS=-fPIC -I${top_srcdir}/src\
	    -I$(TBLIB_ROOT)/include/tbsys\
	    -I$(TBLIB_ROOT)/include/tbnet

AM_LDFLAGS=-lrt -lpthread -ldl

lib_LTLIBRARIES= libtfsclient.la

libtfsclient_la_SOURCES = tfs_file.cpp tfs_large_file.cpp tfs_small_file.cpp tfs_session.cpp fsname.cpp \
                          tfs_session_pool.cpp tfs_client_api.cpp \
                          tfs_file.h tfs_large_file.h tfs_small_file.h tfs_session.h fsname.h \
                          tfs_session_pool.h tfs_client_api.h

libtfsclient_la_LDFLAGS=$(AM_LDFLAGS) \
			$(top_srcdir)/src/message/libtfsmessage.a \
			$(top_srcdir)/src/common/libtfscommon.a \
			$(TBLIB_ROOT)/lib/libtbnet.a \
			$(TBLIB_ROOT)/lib/libtbsys.a

include_HEADERS = tfs_client_api.h

# hardcode to extract static library in destinate static library
# consider libtool compatibility,do change in destinate directory
install-exec-hook:
	cd $(DESTDIR)$(libdir);\
	  client_lib="libtfsclient.a";\
	  tmp_dir=".tfs_tmp_dir";\
	  test -d $$tmp_dir && rm -rf $$tmp_dir;\
	  mkdir -p $$tmp_dir && mv $$client_lib $$tmp_dir;\
	  cd  $$tmp_dir;\
	  ar x $$client_lib;\
	  rm -f $$client_lib;\
	  for i in *.a ; do\
	  lib_tmp_dir=".tmp_$$i";\
	  mkdir -p $$lib_tmp_dir;\
	  mv $$i $$lib_tmp_dir;\
	  cd $$lib_tmp_dir;\
	  ar x $$i;\
	  cd ../; done;\
	  ar cru ../$$client_lib `find . -name '*.o'`;\
	  ranlib ../$$client_lib;\
	  chmod 644 ../$$client_lib;\
	  cd ../ && rm -rf $$tmp_dir

# installed dirctory differ from original src dirctory hierarchy, hardcode to change
install-data-hook:
	cd $(DESTDIR)$(includedir) && sed -i 's#common/\(.*\.h\)#\1#g' $(include_HEADERS)
