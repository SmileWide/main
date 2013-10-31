#!/bin/bash
rm -rf ../target/hive-test
mkdir ../target/hive-test
cd ../target/hive-test
cp ../../input/hivePosteriors.q .
cp ../../input/credit.* .
cp ../smile-wide-0.0.1-SNAPSHOT.jar .
cp ../lib/*.so .
jar xf ../smile-wide-0.0.1-SNAPSHOT-job.jar lib/smile-2013.08.01.jar
mv lib/* .
rm -rf lib
ls -l
echo Hive test files are ready in target/hive-test subdirectory
echo To test Hive UDFs, change the directoru and execute the
echo following command: hive -f hivePosteriors.q 
