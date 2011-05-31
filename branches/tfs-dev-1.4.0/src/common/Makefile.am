AM_CPPFLAGS=-I$(TBLIB_ROOT)/include/tbsys\
	    -I$(TBLIB_ROOT)/include/tbnet

AM_LDFLAGS=-lrt -lpthread -ldl -fPIC

#LDADD=$(TBLIB_ROOT)/lib/libtbnet.a \
#      $(TBLIB_ROOT)/lib/libtbsys.a


noinst_LIBRARIES = libtfscommon.a

libtfscommon_a_SOURCES = internal.cpp parameter.cpp func.cpp file_queue.cpp \
			 file_queue_thread.cpp lock.cpp directory_op.cpp base_packet.cpp\
			 file_op.cpp base_service.cpp new_client.cpp client_manager.cpp\
                         base_packet_streamer.cpp base_packet_factory.cpp\
			 stream.cpp status_message.cpp rc_define.cpp\
			 parameter.h func.h file_queue.h \
			 file_queue_thread.h lock.h directory_op.h \
			 define.h interval.h error_msg.h config_item.h \
			 file_op.h statistics.h atomic.h base_packet_streamer.h\
			 base_packet_factory.h base_packet.h base_service.h rc_define.h cdefine.h

include_HEADERS = define.h cdefine.h
