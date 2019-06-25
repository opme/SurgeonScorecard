#!/bin/sh
export CLASSPATH=$CLASSPATH:/c/users/ext-shambmi/spark-2.4.3-bin-hadoop2.7/jars
export JAVA_OPTS="$JAVA_OPTS -Dhttp.proxyHost=husproxy.hus.fi -Dhttp.proxyPort=8080 -Dhttps.proxyHost=husproxy.hus.fi -Dhttps.proxyPort=8080"export HADOOP_HOME=/c/users/ext-shambmi/winutils
