#!/bin/bash

BIN_PATH=/home/admin/workspace/nameMetaOper
MAVEN_HOME=/usr/local/apache-maven-3.0.3/bin

start_oper()
{
  cd $BIN_PATH;
  ${MAVEN_HOME}/mvn compile && ${MAVEN_HOME}/mvn exec:java -Dexec.mainClass=com.taobao.common.tfs.nameMetaOper.nameMetaOper -Dexec.args="$1"
}

stop_oper()
{
  killall java
}

#------------------------------------------------------------
#--------------------------main------------------------------

case "$1" in
  start_oper)
  start_oper $2
  ;;
  stop_oper)
  stop_oper
  ;;
  *)
  ;;
esac
