RC_DIR=.
OUT_DIR=./out
TFS_SRC=/home/admin/trunk/src
TBLIB_ROOT=/opt/csr/common
SRC=/admin/diqing/tfs_bin/lib
SRC_DIR=.

#complie option
CPP=g++
#ifeq ($(coverage), yes)
#CFLAGS=-Wall -g --coverage $(ICLUDDE) -I$(TFS_SRC) -I$(TFS_SRC)/common -I$(TFS_SRC)/new_client -I$(TFS_SRC)/dataserver -I$(TFS_SRC)/message -I$(TBLIB_ROOT)/include/tbnet -I$(TBLIB_ROOT)/include/tbsys 
#CLINKER = --coverage
#else
CFLAGS=-Wall -g -I$(TFS_SRC) -I$(TFS_SRC)/common -I$(TFS_SRC)/clientv2 -I$(TFS_SRC)/dataserver -I$(TFS_SRC)/message -I$(TBLIB_ROOT)/include/tbnet -I$(TBLIB_ROOT)/include/tbsys 
#endif
LIBFLAGS=-L${SRC} -L${TFS_SRC}/clientv2/ -L${TFS_SRC}/message/ -L${TFS_SRC}/messagev2/ 

LIBS= $(TFS_SRC)/clientv2/.libs/libtfsclientv2.a $(TFS_SRC)/message/libtfsmessage.a $(TFS_SRC)/common/libtfscommon.a $(TBLIB_ROOT)/lib/libtbnet.a $(TBLIB_ROOT)/lib/libtbsys.a -lpthread -lgtest  -lpthread -lrt  -luuid -lz


#obj=test_02_put_bucket test_03_get_bucket test_04_head_bucket test_05_del_bucket test_06_put_object test_07_get_object test_08_head_object test_09_del_object test_10_pwrite_object test_11_pread_object test_12_put_bucket_acl test_13_put_bucket_acl_map test_14_get_bucket_acl test_15_put_bucket_logging test_16_get_bucket_logging test_17_put_bucket_tag test_18_get_bucket_tag test_19_del_bucket_tag test_21_get_service
obj= test_01_initialize test_02_put_bucket test_03_get_bucket test_04_head_bucke  test_05_del_bucket test_06_put_object test_07_get_object ttest_08_head_object test_09_del_object test_10_pwrite_object test_11_pread_object 

.PHONY: clear all  tear

all: mkdir $(obj) move
		@echo "make finished !"
mkdir:
			@rm -rf out
			@mkdir out

$(obj): % : %.cpp
		@${CPP} -o $@  -lgtest ${CFLAGS} $< ${LIBFLAGS} ${LIBS}

clear:
		@rm  -rf *.o
		@rm  -rf ./out
		@rm -rf *.gcda
		@rm -rf *.gcno
tear:
		@rm  -rf *.o

move:
		@mv ${SRC_DIR}/$(obj)  ${OUT_DIR}


