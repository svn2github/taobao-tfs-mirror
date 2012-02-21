#!/bin/bash

tfsHome=/home/admin/workspace/lexin.wxx/tfs-dev-2.0.1
tfsbin=/home/admin/workspace/lexin.wxx/tfs_bin
tfsBinHome=/home/admin/workspace/lexin.wxx/

#tfs_bin install path
tfsbin_install=/home/admin/tfs_bin

`export TBLIB_ROOT=/home/admin/tb-common-utils`

#define startServer and stopServer

kill_servers()
{
    `ps x|egrep "${tfsbin}/bin/(meta|rc|root|name|data)server"|awk '{print $5,$6,$7,$8,$9}' > 1.lst`
    `ps x|egrep "${tfsbin}/bin/(meta|rc|root|name|data)server"|awk '{print $1}'|xargs kill -9`
}
start_servers()
{
    `sh 1.lst`
}


#start update tfsclient
cd ${tfsHome} ; svn up

sh ${tfsHome}/build.sh clean
sh ${tfsHome}/build.sh init

sed -i s/5.1.48/5.0.1/g ${tfsHome}/configure
sh ${tfsHome}/configure
cd ${tfsHome} ; make
cd ${tfsHome} ; make install

kill_servers

#update tfs_bin
cp -r ${tfsbin_install} ${tfsBinHome}

#rm log
rm ${tfsbin}/logs/* -f

start_servers
