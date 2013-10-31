#!/bin/bash

THISDIR=`pwd`
ETCHADOOP=/etc/hadoop/conf.compute/
HIVEPATH="/etc/hadoop/software/hive/lib/*"
OPTS="-Ddfs.permission=false -Dmapreduce.jobtracker.staging.root.dir=/user/tsingliar"
OPTS="$OPTS -Dmapred.max.split.size=$((4 * 102 * 1024))"
OPTS="$OPTS -Dmapred.min.split.size=$((3 * 102 * 1024))"
OPTS="$OPTS -Dmapred.map.tasks=200 -Dmapred.reduce.tasks=10"
# INFILE=/user/tsingliar/FB_Users_514202600_to_550000000_pulled_20121011.csv
INFILE=/user/tsingliar/FB_Users_34579801_to_50000000_pulled_2012-10-03.csv
NETWORK=$THISDIR/input/Facebook.xdsl
LIBPATH=$THISDIR/lib/linux64
OUTDIR=/user/tsingliar/bayesnets/out/200

export HADOOP_CLASSPATH=$HIVEPATH:$HADOOP_CLASSPATH
export HADOOP_CLIENT_OPTS="$HADOOP_CLIENT_OPTS -Djava.library.path=$LD_LIBRARY_PATH:$LIBPATH"

echo Native library path is $LD_LIBRARY_PATH
echo hadoop jar jParInf.jar $CONF $OPTS $NETWORK $INFILE $OUTDIR

hadoop fs -rm -r $OUTDIR
hadoop jar jParInf.jar $CONF $OPTS $NETWORK $INFILE $OUTDIR

