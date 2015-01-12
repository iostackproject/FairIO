#!/bin/bash
./start-all.sh
./ganglia-on.sh
hdfs dfs -setfattr -n "user.weight" -v 100 /
