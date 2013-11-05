#!/bin/bash
THISDIR=`pwd`
MYJAR="$THISDIR/../target/smile-wide-0.0.1-SNAPSHOT-job.jar"
export JAVA_LIBRARY_PATH=$THISDIR/../target/lib
hadoop jar $MYJAR smile.wide.algorithms.pc.PC
