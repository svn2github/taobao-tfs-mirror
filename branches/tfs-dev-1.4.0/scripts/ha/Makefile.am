pkgdata_datadir=${prefix}/scripts/ha
pkgdata_data_DATA=authkeys.sh deploy.sh ha.cf NameServer nsdep.sh ns.xml

install-data-hook:
	mv ${DESTDIR}/${prefix}/scripts/ha/authkeys.sh ${DESTDIR}/${prefix}/scripts/ha/authkeys
	mv ${DESTDIR}/${prefix}/scripts/ha/deploy.sh ${DESTDIR}/${prefix}/scripts/ha/deploy
	mv ${DESTDIR}/${prefix}/scripts/ha/nsdep.sh ${DESTDIR}/${prefix}/scripts/ha/nsdep
	chmod u+x ${DESTDIR}/${prefix}/scripts/ha/authkeys
	chmod u+x ${DESTDIR}/${prefix}/scripts/ha/deploy
	chmod u+x ${DESTDIR}/${prefix}/scripts/ha/nsdep
	chmod u+x ${DESTDIR}/${prefix}/scripts/ha/NameServer
