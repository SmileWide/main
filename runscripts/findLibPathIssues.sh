#!/bin/bash

THISDIR=`pwd`
ETCHADOOP=/etc/hadoop/conf.compute/
HIVEPATH="/etc/hadoop/software/hive/lib/*"
# CONF="-conf $ETCHADOOP/core-site.xml -conf $ETCHADOOP/mapred-site.xml -conf $ETCHADOOP/yarn-site.xml"
OPTS="-Ddfs.permission=false -Dmapreduce.jobtracker.staging.root.dir=/user -Dmapred.reduce.tasks=10"
OPTS="$OPTS -Dmapred.max.split.size=$((10*1024*1024))"
INFILE=/user/tsingliar/FB_Users_514202600_to_550000000_pulled_20121011.csv
NETWORK=$THISDIR/input/Facebook.xdsl
LIBPATH=$THISDIR/lib/linux64
OUTDIR=/user/bigdata/bayesnets/out/1


java -jar testjSmile.jar testjSmile.jar
echo Plain plain java OK

LD_LIBRARY_PATH=$LIBPATH:$LD_LIBRARY_PATH
java -jar testjSmile.jar -Djava.library.path=$LD_LIBRARY_PATH
echo Libpath java OK

export LD_LIBRARY_PATH
export HADOOP_CLIENT_OPTS="$HADOOP_CLIENT_OPTS -Djava.library.path=$LD_LIBRARY_PATH"

echo Native library path is $LD_LIBRARY_PATH
java -jar testjSmile.jar 
echo Plain java OK with env var

hadoop jar testjSmile.jar $CONF $OPTS $NETWORK $INFILE $OUTDIR
echo hadpoop hadppy

