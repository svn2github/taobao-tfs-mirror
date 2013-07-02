AM_CPPFLAGS=-fPIC -I${top_srcdir}/src\
	    -I$(TBLIB_ROOT)/include/tbsys\
	    -I$(TBLIB_ROOT)/include/tbnet

AM_LDFLAGS=-lrt -lpthread -ldl -luuid 

lib_LTLIBRARIES= libtfsclientv2.la 

api_source_list = tfs_client_api_v2.cpp tfs_client_impl_v2.cpp tfs_file.cpp fsname.cpp \
									tfs_client_api_v2.h tfs_client_impl_v2.h tfs_file.h fsname.h encode.h

ld_fg = $(AM_LDFLAGS) \
        $(top_srcdir)/src/message/libtfsmessage.a \
        $(top_srcdir)/src/common/libtfscommon.a

libtfsclientv2_la_SOURCES = ${api_source_list}
libtfsclientv2_la_LIBADD = ${ld_fg}

#bin_PROGRAMS = test_tfs_client
#test_tfs_client_SOURCES  = test_tfs_client.cpp ${api_source_list}

include_HEADERS = tfs_client_api_v2.h

# hardcode to extract static library in destinate static library
# consider libtool compatibility,do change in destinate directory
install-exec-hook:
	  tmp_dir=".tfs_tmp_dir";\
	  for client_lib in libtfsclientv2.a; \
	  do \
	    cd $(DESTDIR)$(libdir);\
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
	  done; \
    cd .. && rm -rf $$tmp_dir

# installed dirctory differ from original src dirctory hierarchy, hardcode to change
install-data-hook:
	cd $(DESTDIR)$(includedir) && \
        sed -i 's#common/\(.*\.h\)#\1#g' ${include_HEADERS}
