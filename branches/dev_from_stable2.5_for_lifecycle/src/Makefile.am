AUTOMAKE_OPTIONS=foreign

dir_base = common message new_client clientv2 dataserver nameserver adminserver \
rcserver monitor name_meta_server rootserver kv_meta_server checkserver tools 
if WITH_LIFECYCLE
SUBDIRS = ${dir_base} life_cycle_manager
else
SUBDIRS = ${dir_base}
endif
