#!/bin/bash

hadoop jar ../share/hadoop/hdfs/hadoop-hdfs-2.5.1-tests.jar org.apache.hadoop.fs.TestDFSIO -Dtest.build.data=/$1 -read -nrFiles $2 -fileSize $3
