OUT_DIR=./out
TFS_SRC=/home/xueya.yy/tfs-client-cpp-restful
TBLIB_ROOT=/home/xueya.yy/lib
SRC_DIR=.

#complie option
CPP=g++

CFLAGS=-Wall -g -I$(TFS_SRC) -I$(TBLIB_ROOT)/include/tbnet -I$(TBLIB_ROOT)/include/tbsys 

LIBFLAGS=-L${TFS_SRC} 

LIBS= $(TFS_SRC)/libcpp_restful_client.a $(TBLIB_ROOT)/lib/libtbnet.a $(TBLIB_ROOT)/lib/libtbsys.a -lpthread -lpthread -lrt -luuid -lz -lcurl -ljson


obj=PerfRestMain


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

move:
		@mv ${SRC_DIR}/$(obj)  ${OUT_DIR}

