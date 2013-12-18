#!/bin/bash
THISDIR=`pwd`
MYJAR="$THISDIR/../target/smile-wide-0.0.1-SNAPSHOT-job.jar"
MEMORY="-Dmapred.map.child.java.opts=-Xmx1G"
export HADOOP_CLIENT_OPTS="$HADOOP_CLIENT_OPTS $MEMORY"
export HADOOP_USER_CLASSPATH_FIRST=true
export JAVA_LIBRARY_PATH=$THISDIR/../target/lib
hadoop jar $MYJAR smile.wide.algorithms.pc.PC 

