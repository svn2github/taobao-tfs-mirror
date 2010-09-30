#!/bin/bash
##for check
export temppath=$1
cd $temppath
#make
cd rpm
/usr/local/bin/rpm_create -p /home/admin -v $3 -r $4 $2.spec