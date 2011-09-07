#!/bin/sh

LD_LIBRARY_PATH=/usr/local/staf/lib:${LD_LIBRARY_PATH}
MAVEN_HOME=$(mvn -v | grep "Maven home" | awk '{print $3}')
JAVA_HOME=$(mvn -v | grep "Java home" | awk '{print $3}')
PATH=$MAVEN_HOME/bin:$JAVA_HOME:${PATH}
CLASSPATH=/usr/local/staf/lib/JSTAF.jar:${CLASSPATH}
export LD_LIBRARY_PATH MAVEN_HOME PATH JAVA_HOME CLASSPATH
#env
mvn $1
