#!/bin/bash

THISDIR=`pwd`
ETCHADOOP=/etc/hadoop/conf.compute/
HIVEPATH="/etc/hadoop/software/hive/lib/*"
OPTS="-Ddfs.permission=false -Dmapreduce.jobtracker.staging.root.dir=/user/tsingliar"
INFILE=/user/tsingliar/FB_Users_34579801_to_50000000_pulled_2012-10-03.csv
NETWORK=$THISDIR/input/Facebook.34579801_to_50000000.xdsl
LIBPATH=$THISDIR/lib/linux64
OUTDIR=/user/tsingliar/bayesnets/out/1
JARFILE=smilewide.jar

export HADOOP_CLASSPATH=$HIVEPATH:$HADOOP_CLASSPATH
export HADOOP_CLIENT_OPTS="$HADOOP_CLIENT_OPTS -Djava.library.path=$LD_LIBRARY_PATH:$LIBPATH"

echo Native library path is $LD_LIBRARY_PATH
echo hadoop jar $JARFILE $CONF $OPTS $NETWORK $INFILE $OUTDIR

hadoop fs -rm -r $OUTDIR
hadoop jar $JARFILE $CONF $OPTS $NETWORK $INFILE $OUTDIR

