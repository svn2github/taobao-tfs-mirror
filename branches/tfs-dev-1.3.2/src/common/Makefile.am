AM_CPPFLAGS=-I$(TBLIB_ROOT)/include/tbsys\
	    -I$(TBLIB_ROOT)/include/tbnet

AM_LDFLAGS=-lrt -lpthread -ldl -fPIC

LDADD=$(TBLIB_ROOT)/lib/libtbnet.a \
      $(TBLIB_ROOT)/lib/libtbsys.a


noinst_LIBRARIES = libtfscommon.a

libtfscommon_a_SOURCES = config.cpp parameter.cpp func.cpp file_queue.cpp \
			 file_queue_thread.cpp lock.cpp directory_op.cpp \
			 config.h parameter.h func.h file_queue.h \
			 file_queue_thread.h lock.h directory_op.h \
			 define.h interval.h error_msg.h config_item.h


include_HEADERS = define.h
