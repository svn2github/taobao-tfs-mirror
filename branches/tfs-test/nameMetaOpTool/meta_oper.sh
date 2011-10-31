#!/bin/bash

BIN_PATH=/home/admin/workspace/tfs.dev/tfs-test/nameMetaOper
MAVEN_HOME=/home/admin/workspace/tfs.dev/apache-maven-3.0.1/bin

start_oper()
{
  cd $BIN_PATH;
  ${MAVEN_HOME}/mvn compile && ${MAVEN_HOME}/mvn exec:java -Dexec.mainClass=com.taobao.common.tfs.function.nameMetaOper -Dexec.args="$1 $2"
}

stop_oper()
{
  killall java
}

#------------------------------------------------------------
#--------------------------main------------------------------

case "$1" in
  start_oper)
  start_oper $2 $3
  ;;
  stop_oper)
  stop_oper
  ;;
  *)
  ;;
esac
