AUTOMAKE_OPTIONS=foreign

SUBDIRS = src conf scripts 
pkgdata_datadir=${prefix}/logs
pkgdata_data_DATA=

DIST_SUBDIRS = src conf scripts tests
EXTRA_DIST=build.sh \
	   conf/ns.conf conf/ds.conf conf/ads.conf conf/Makefile.am conf/Makefile.in \
	   scripts/Makefile.am scripts/Makefile.in scripts/tfs.sh scripts/stfs.sh \
	   libtool \
	   packages/tfs.spec

TMP_DIR := /tmp/tfs-tmp.$(shell echo $$$$)
PREFIX := $(shell [ x$(PREFIX) = x ] && PREFIX=/usr/local/tfs-$(VERSION) && echo "$${PREFIX}")
rpms: dist-gzip packages/tfs.spec
	mkdir -p $(TMP_DIR)
	mkdir -p $(TMP_DIR)/BUILD
	mkdir -p $(TMP_DIR)/RPMS
	mkdir -p $(TMP_DIR)/SOURCES
	mkdir -p $(TMP_DIR)/SRPMS
	cp tfs-${VERSION}.tar.gz $(TMP_DIR)/SOURCES
	echo $(PREFIX)
	cd $(TMP_DIR)/BUILD; \
	  tar xfz $(TMP_DIR)/SOURCES/tfs-${VERSION}.tar.gz \
	  tfs-$(VERSION)/packages/tfs.spec
	rpmbuild --define "_topdir $(TMP_DIR)" \
	  --define "NAME $(PACKAGE)" \
	  --define "VERSION $(VERSION)" \
	  --define "_prefix $(PREFIX)" \
	  -ba $(TMP_DIR)/BUILD/tfs-$(VERSION)/packages/tfs.spec
	find $(TMP_DIR)/RPMS/ -name "*.rpm" -exec mv '{}' . \;
	rm -rf $(TMP_DIR)
