#!/bin/bash
echo "Propagar projecte Hadoop a rack"
ssh hadoop@stacksync.urv.cat rm -r /home/hadoop/hadoop-dir/hadoop-2.5.1/
scp -r "$HADOOP_HOME" hadoop@stacksync.urv.cat:/home/hadoop/hadoop-dir/

echo "Propagar fitxers configuracio"
./deploy_config_files_to_RACK.sh

echo " "
echo "Tot replicat i preparat"
