#!/bin/bash
THISDIR=`pwd`
MYJAR="$THISDIR/../dist/lib/SMILE-WIDE-0.1-20130603.jar"
export HADOOP_CLASSPATH=$THISDIR/../lib/linux64/smile.jar
export JAVA_LIBRARY_PATH=$THISDIR/../lib/linux64/
hadoop jar $MYJAR smile.wide.algorithms.BayesianSearchSMILEHadoop $THISDIR/../input/mushroom_train.txt 1000
