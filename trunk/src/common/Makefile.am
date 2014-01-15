AM_CPPFLAGS=-I$(TBLIB_ROOT)/include/tbsys\
	    -I$(TBLIB_ROOT)/include/tbnet\
	    $(TAIR_META_CPPFLAGS) -fpic

AM_LDFLAGS=-lrt -lpthread -ldl $(TAIR_META_LDFLAGS)

noinst_LIBRARIES = libtfscommon.a

if WITH_KV_META
tair_kv_source=kvengine_helper.h kvengine_helper.cpp tairengine_helper.cpp tairengine_helper.h
endif

libtfscommon_a_SOURCES = internal.cpp parameter.cpp func.cpp file_queue.cpp \
			 file_queue_thread.cpp lock.cpp directory_op.cpp base_packet.cpp http_packet.cpp version.cpp\
			 base_main.cpp base_service.cpp new_client.cpp client_manager.cpp meta_define.cpp \
       http_packet_streamer.cpp base_packet_streamer.cpp base_packet_factory.cpp  http_message_factory.cpp\
			 meta_server_define.cpp session_util.cpp http_agent.cpp http_packet_streamer.cpp base_http_message.cpp\
			 stream.cpp status_message.cpp rc_define.cpp mmap_file.cpp rts_define.cpp expire_define.cpp meta_hash_helper.cpp \
			 tfs_vector.ipp file_opv2.cpp mmap_file_op.cpp file_op.cpp kv_meta_define.cpp kv_rts_define.cpp ${tair_kv_source}\
       file_opv2.h file_op.h meta_server_define.h  tfs_vector.h version.h base_http_message.h http_message_factory.h\
			 atomic.h base_main.h http_packet_streamer.h base_packet_factory.h base_packet.h http_packet.h base_packet_streamer.h \
			 base_service.h buffer.h cdefine.h client_manager.h config_item.h define.h http_packet_streamer.h\
       directory_op.h error_msg.h file_queue.h file_queue_thread.h func.h base_object.h\
       internal.h local_packet.h lock.h new_client.h parameter.h cdefine.h rc_define.h serialization.h\
			 statistics.h status_message.h stream.h session_util.h rts_define.h mmap_file.h meta_hash_helper.h\
			 array_helper.h meta_define.h kv_rts_define.h mmap_file_op.h expire_define.h http_agent.h
include_HEADERS = define.h cdefine.h lock.h  func.h internal.h meta_define.h meta_server_define.h rts_define.h error_msg.h kv_meta_define.h kv_rts_define.h
			 array_helper.h mmap_file_op.h kvengine_helper.h tairengine_helper.h expire_define.h 
SUBDIRS = mysql_cluster
