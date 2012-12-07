#dir
SRC_DIR=.
OUT_DIR=./out
TFS_SRC=/home/admin/stable-2.1/src
TBLIB_ROOT=/home/admin/tbnet/
SRC=/lib


#complie option
CPP=g++

CFLAGS=-Wall -g -I$(TFS_SRC) -I$(TFS_SRC)/common -I$(TFS_SRC)/new_client -I$(TFS_SRC)/dataserver -I$(TFS_SRC)/message -I$(TBLIB_ROOT)/include/tbnet -I$(TBLIB_ROOT)/include/tbsys 

LIBFLAGS=-L${SRC} -L${TFS_SRC}/client/ -L${TFS_SRC}/message/ -L${TFS_SRC}/messagev2/

LIBS= $(TFS_SRC)/new_client/.libs/libtfsclient.a $(TFS_SRC)/message/libtfsmessage.a $(TFS_SRC)/common/libtfscommon.a $(TBLIB_ROOT)/lib/libtbnet.a $(TBLIB_ROOT)/lib/libtbsys.a -lpthread -lgtest  -lpthread -lrt  -luuid -lz

#relative
obj=test_01_create_dir test_02_create_file test_03_rm_dir test_04_rm_file test_05_mv_dir test_06_mv_file test_07_ls_dir test_08_ls_file  test_09_open test_10_read test_11_write test_12_close test_13_save_file test_14_save_buf test_15_fetch_file test_16_fetch_buf test_17_exist_file test_18_exist_dir

#obj=test_16_fetch_buf



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


