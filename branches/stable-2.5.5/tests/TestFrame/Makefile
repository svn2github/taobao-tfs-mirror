OUT_DIR=./out
TFS_SRC=/home/xueya.yy/tfs-client-cpp-restful/src
TFS_BIN=/home/xueya.yy/tfs_bin
TBLIB_ROOT=/opt/csr/common
SRC_DIR=.

#complie option
CPP=g++

CFLAGS=-Wall -g -I$(TFS_BIN)/include -I$(TFS_SRC) -I$(TBLIB_ROOT)/include/tbnet -I$(TBLIB_ROOT)/include/tbsys 

LIBFLAGS=-L${TFS_SRC} 

LIBS= $(TFS_SRC)/libtfs_cpp_restful_client.a $(TFS_BIN)/lib/libtfsclient.a $(TBLIB_ROOT)/lib/libtbsys.a $(TBLIB_ROOT)/lib/libtbnet.a -lpthread -lrt -luuid -lz -lcurl -ljson


obj=PerfMain


.PHONY: clean all  tear

all: mkdir $(obj) move
		@echo "make finished !"

mkdir:
		@rm -rf out
		@mkdir out

$(obj): % : %.cpp
		@${CPP} -o $@  -lgtest ${CFLAGS} $< ${LIBFLAGS} ${LIBS}

clean:
		@rm  -rf *.o
		@rm  -rf ./out

tear:
		@rm  -rf *.o

move: $(obj)
		@mv ${SRC_DIR}/$(obj)  ${OUT_DIR}

