pkgdata_datadir=${prefix}/scripts
pkgdata_data_DATA=stfs.sh tfs.sh

install-data-hook:
	mv ${DESTDIR}/${prefix}/scripts/stfs.sh ${DESTDIR}/${prefix}/scripts/stfs
	mv ${DESTDIR}/${prefix}/scripts/tfs.sh ${DESTDIR}/${prefix}/scripts/tfs
	chmod u+x ${DESTDIR}/${prefix}/scripts/stfs
	chmod u+x ${DESTDIR}/${prefix}/scripts/tfs
	sed -i 's#\(TFS_HOME=\).*$$#\1${prefix}#g' ${DESTDIR}/${prefix}/scripts/stfs ${DESTDIR}/${prefix}/scripts/tfs