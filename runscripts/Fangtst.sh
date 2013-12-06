#!/bin/bash
THISDIR=`pwd`
MATH3="/home/bigdata/.m2/repository/org/apache/commons/commons-math3/3.0"
MYJAR="$THISDIR/../target/smile-wide-0.0.1-SNAPSHOT-job.jar"
export JAVA_LIBRARY_PATH=$THISDIR/../target/lib
export HADOOP_CLASSPATH=$MATH3/commons-math3-3.0.jar
hadoop jar $MYJAR smile.wide.algorithms.fang.FangJob -libjars $MATH3/commons-math3-3.0.jar
