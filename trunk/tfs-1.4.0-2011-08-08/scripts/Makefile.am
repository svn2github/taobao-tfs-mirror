pkgdata_datadir=${prefix}/scripts
hadir=$(pkgdata_datadir)/ha
ha_DATA=ha/authkeys.sh ha/deploy.sh ha/ha.cf ha/NameServer ha/nsdep.sh ha/ns.xml
pkgdata_data_DATA=stfs.sh tfs.sh 

install-data-hook:
	mv ${DESTDIR}/${prefix}/scripts/ha/authkeys.sh ${DESTDIR}/${prefix}/scripts/ha/authkeys
	mv ${DESTDIR}/${prefix}/scripts/stfs.sh ${DESTDIR}/${prefix}/scripts/stfs
	mv ${DESTDIR}/${prefix}/scripts/tfs.sh ${DESTDIR}/${prefix}/scripts/tfs
	mv ${DESTDIR}/${prefix}/scripts/ha/deploy.sh ${DESTDIR}/${prefix}/scripts/ha/deploy
	mv ${DESTDIR}/${prefix}/scripts/ha/nsdep.sh ${DESTDIR}/${prefix}/scripts/ha/nsdep
	chmod u+x ${DESTDIR}/${prefix}/scripts/stfs
	chmod u+x ${DESTDIR}/${prefix}/scripts/tfs
	chmod u+x ${DESTDIR}/${prefix}/scripts/ha/authkeys
	chmod u+x ${DESTDIR}/${prefix}/scripts/ha/deploy
	chmod u+x ${DESTDIR}/${prefix}/scripts/ha/nsdep
	chmod u+x ${DESTDIR}/${prefix}/scripts/ha/NameServer
	sed -i 's#\(TFS_HOME=\).*$$#\1${prefix}#g' ${DESTDIR}/${prefix}/scripts/stfs ${DESTDIR}/${prefix}/scripts/tfs
