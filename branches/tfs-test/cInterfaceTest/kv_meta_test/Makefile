RC_DIR=.
OUT_DIR=./out
TFS_SRC=/home/yiming.czw/dev_tair_meta_from_2.2.12/src
TBLIB_ROOT=/home/yiming.czw/trunk/
SRC=/lib
SRC_DIR=.

#complie option
CPP=g++

CFLAGS=-Wall -g -I$(TFS_SRC) -I$(TFS_SRC)/common -I$(TFS_SRC)/new_client -I$(TFS_SRC)/dataserver -I$(TFS_SRC)/message -I$(TBLIB_ROOT)/include/tbnet -I$(TBLIB_ROOT)/include/tbsys 

LIBFLAGS=-L${SRC} -L${TFS_SRC}/client/ -L${TFS_SRC}/message/ -L${TFS_SRC}/messagev2/

LIBS= $(TFS_SRC)/new_client/.libs/libtfsclient.a $(TFS_SRC)/message/libtfsmessage.a $(TFS_SRC)/common/libtfscommon.a $(TBLIB_ROOT)/lib/libtbnet.a $(TBLIB_ROOT)/lib/libtbsys.a -lpthread -lgtest  -lpthread -lrt  -luuid -lz


obj=test_02_put_bucket test_03_get_bucket test_04_head_bucket test_05_del_bucket test_06_put_object test_07_get_object test_08_head_object test_09_del_object test_10_pwrite_object test_11_pread_object


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

tear:
		@rm  -rf *.o

move:
		@mv ${SRC_DIR}/$(obj)  ${OUT_DIR}


