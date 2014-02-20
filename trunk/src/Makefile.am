AUTOMAKE_OPTIONS=foreign

if WITH_LIFECYCLE
SUBDIRS = common message requester clientv2 dataserver nameserver \
	rcserver monitor kv_meta_server checkserver syncserver migrateserver kv_meta_server \
	life_cycle_manager tools
else
SUBDIRS = common message requester clientv2 dataserver nameserver \
	rcserver monitor kv_meta_server checkserver syncserver migrateserver kv_meta_server \
	tools
endif
