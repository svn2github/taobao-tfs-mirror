#dir
SRC_DIR=.
OUT_DIR=./out
TFS_BIN=/home/admin/tfs_bin
TBLIB_ROOT=/opt/csr/common
TFS_SRC=/home/admin/tfstrunk/src
    
#complie option
CPP=g++
       
CFLAGS=-Wall -g -I$(TFS_BIN)/include -I$(TBLIB_ROOT)/include/tbnet -I$(TBLIB_ROOT)/include/tbsys -I$(TFS_SRC)/common
        
LIBFLAGS= -L${TFS_BIN}/lib
	 
LIBS= $(TFS_BIN)/lib/libtfsclient.a $(TBLIB_ROOT)/lib/libtbnet.a $(TBLIB_ROOT)/lib/libtbsys.a -lpthread -lgtest  -lpthread -lrt -luuid -lz
#relative
obj=test_01_initialize test_02_initialize_uint64 test_03_open test_04_close test_05_read test_06_readv2 test_07_write test_08_lseek test_09_unlink test_10_save_file test_11_save_buf test_12_logout test_13_fstat test_14_fetch_file test_15_fetch_buf

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


