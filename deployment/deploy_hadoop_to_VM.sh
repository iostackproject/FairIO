#!/bin/bash
echo "Assegurar hadoop apagat"
ssh -p 2100 -l hadoop localhost /home/hadoop/hadoop-dir/hadoop-2.5.1/sbin/stop-all.sh

echo "Propagar projecte Hadoop 2.5.1 a hadoop100"
ssh -p 2100 -l hadoop localhost rm -r /home/hadoop/hadoop-dir/hadoop-2.5.1
scp -r -P 2100 "$HADOOP_HOME" hadoop@localhost:/home/hadoop/hadoop-dir/

echo "Propagar projecte Hadoop 2.5.1 a hadoop101"
ssh -p 2101 -l hadoop localhost rm -r /home/hadoop/hadoop-dir/hadoop-2.5.1
scp -r -P 2101 "$HADOOP_HOME" hadoop@localhost:/home/hadoop/hadoop-dir/

echo "Propagar projecte Hadoop 2.5.1 a hadoop102"
ssh -p 2102 -l hadoop localhost rm -r /home/hadoop/hadoop-dir/hadoop-2.5.1
scp -r -P 2102 "$HADOOP_HOME" hadoop@localhost:/home/hadoop/hadoop-dir/

echo "Propagar fitxers configuracio"
./deploy_config_files_to_VM.sh


echo "Formatar namenode"
ssh -p 2100 -l hadoop localhost /home/hadoop/hadoop-dir/hadoop-2.5.1/bin/hdfs namenode -format

echo " "
echo "Tot replicat i preparat"
