AM_CPPFLAGS=-I${top_srcdir}/src\
	    -I$(TBLIB_ROOT)/include/tbsys\
	    -I$(TBLIB_ROOT)/include/tbnet\
			$(TAIR_CACHE_CPPFLAGS)

AM_LDFLAGS=-lrt -lpthread -ldl -luuid $(TAIR_CACHE_LDFLAGS) 

if WITH_TAIR_CACHE
tair_cache_source=tair_cache_helper.cpp tair_cache_helper.h
endif

lib_LTLIBRARIES= libtfsclientv2.la 

api_source_list = tfs_client_api_v2.cpp tfs_client_impl_v2.cpp tfs_file.cpp fsname.cpp \
									tfs_session.cpp tfs_session_pool.cpp client_config.cpp \
									tfs_client_api_v2.h tfs_client_impl_v2.h tfs_file.h fsname.h encode.h \
									tfs_session.h tfs_session_pool.h lru.h client_config.h \
                  $(tair_cache_source)

ld_fg = $(AM_LDFLAGS) \
        $(top_srcdir)/src/requester/libtfsrequester.a \
        $(top_srcdir)/src/message/libtfsmessage.a \
        $(top_srcdir)/src/common/libtfscommon.a

libtfsclientv2_la_SOURCES = ${api_source_list}
libtfsclientv2_la_LIDADD = ${ld_fg}


# needed by test_tfs_client
#LDADD=$(top_builddir)/src/message/libtfsmessage.a\
#      $(top_builddir)/src/common/libtfscommon.a\
#      $(TBLIB_ROOT)/lib/libtbnet.a\
#      $(TBLIB_ROOT)/lib/libtbsys.a

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
