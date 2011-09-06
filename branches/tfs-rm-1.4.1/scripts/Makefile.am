pkgdata_datadir=${prefix}/scripts
pkgdata_data_DATA=stfs.sh tfs.sh 

SUBDIRS = ha

DIST_SUBDIRS = ha

EXTRA_DIST=ha/authkeys.sh ha/deploy.sh ha/ha.cf ha/NameServer ha/nsdep.sh ha/ns.xml

install-data-hook:
	mv ${DESTDIR}/${prefix}/scripts/stfs.sh ${DESTDIR}/${prefix}/scripts/stfs
	mv ${DESTDIR}/${prefix}/scripts/tfs.sh ${DESTDIR}/${prefix}/scripts/tfs
	chmod u+x ${DESTDIR}/${prefix}/scripts/stfs
	chmod u+x ${DESTDIR}/${prefix}/scripts/tfs
	sed -i 's#\(TFS_HOME=\).*$$#\1${prefix}#g' ${DESTDIR}/${prefix}/scripts/stfs ${DESTDIR}/${prefix}/scripts/tfs
