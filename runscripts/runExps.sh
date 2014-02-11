#!/bin/bash
THISDIR=`pwd`
MYJAR="$THISDIR/../target/smile-wide-0.0.1-SNAPSHOT-job.jar"
MATH3="/home/bigdata/.m2/repository/org/apache/commons/commons-math3/3.0"
LANG3="/home/bigdata/.m2/repository/org/apache/commons/commons-lang3/3.1"
export JAVA_LIBRARY_PATH=$THISDIR/../target/lib
export HADOOP_CLASSPATH=$MATH3/commons-math3-3.0.jar:$LANG3/commons-lang3-3.1.jar
export HADOOP_CLIENT_OPTS="$HADOOP_CLIENT_OPTS -Xmx2048m"
export HADOOP_USER_CLASSPATH_FIRST=true

hadoop jar $MYJAR smile.wide.utils.ExperimentCode -libjars $MATH3/commons-math3-3.0.jar $1 $2


