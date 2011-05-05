all:
	g++ `mysql_config --include` -I ../ -I$(TBLIB_ROOT)/include/tbsys \
		database_helper.cpp mysql_database_helper.cpp data_test.cpp \
		`mysql_config --libs` \
      $(TBLIB_ROOT)/lib/libtbnet.a \
      $(TBLIB_ROOT)/lib/libtbsys.a -lpthread -lrt
