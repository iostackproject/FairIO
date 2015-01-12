#!/bin/bash
./start-all.sh
#./ganglia-on.sh
hdfs dfsadmin -safemode leave
hdfs dfs -setfattr -n "user.weight" -v 100 /
