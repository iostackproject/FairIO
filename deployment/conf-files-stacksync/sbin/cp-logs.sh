#!/bin/bash

mkdir ../../../$1
cp -rf $HADOOP_LOGGS/* ../../../$1
scp -rf hadoop@storage0:$HADOOP_LOGGS/* ../../../$1
scp -rf hadoop@storage0:$HADOOP_HOME/datanode-metrics.out ../../../$1

rm -rf $HADOOP_LOGGS/*
ssh hadoop@storage0 "rm -rf $HADOOP_LOGGS/*"
ssh hadoop@storage0 "rm -rf $HADOOP_HOME/datanode-metrics.out"
