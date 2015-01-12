#!/bin/bash

hdfs dfs -mkdir /in100
hdfs dfs -mkdir /in200
hdfs dfs -mkdir /in400
hdfs dfs -setfattr -n "user.weight" -v 200 /in200
hdfs dfs -setfattr -n "user.weight" -v 400 /in400
hdfs dfs -setfattr -n "user.weight" -v 100 /in100

hdfs dfs -Ddfs.block.size=1048576 -put ../../file100Mb.txt /in400
hdfs dfs -Ddfs.block.size=1048576 -put ../../file100Mb.txt /in200
hdfs dfs -Ddfs.block.size=1048576 -put ../../file100Mb.txt /in100

