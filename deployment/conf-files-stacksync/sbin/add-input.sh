#!/bin/bash
hdfs dfs -mkdir /input
hdfs dfs -put ../../4300.txt /input
hdfs dfs -put ../../zorkuu.txt /input
hdfs dfs -put ../../zs.txt /input
hdfs dfs -put ../../zorro.txt /input

hdfs dfs -ls /input
