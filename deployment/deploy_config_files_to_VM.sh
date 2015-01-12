#!/bin/bash

echo "Propagar conf a hadoop100"
scp -r -P 2100 /home/beto/hadoop-2.5.1/conf-files-backup/hadoop/ hadoop@localhost:/home/hadoop/hadoop-dir/hadoop-2.5.1/etc
scp -r -P 2100 /home/beto/hadoop-2.5.1/conf-files-backup/libexec/ hadoop@localhost:/home/hadoop/hadoop-dir/hadoop-2.5.1
scp -r -P 2100 /home/beto/hadoop-2.5.1/conf-files-backup/sbin/ hadoop@localhost:/home/hadoop/hadoop-dir/hadoop-2.5.1

echo "Propagar conf a hadoop101"
scp -r -P 2101 /home/beto/hadoop-2.5.1/conf-files-backup/hadoop/ hadoop@localhost:/home/hadoop/hadoop-dir/hadoop-2.5.1/etc
scp -r -P 2101 /home/beto/hadoop-2.5.1/conf-files-backup/libexec/ hadoop@localhost:/home/hadoop/hadoop-dir/hadoop-2.5.1
scp -r -P 2101 /home/beto/hadoop-2.5.1/conf-files-backup/sbin/ hadoop@localhost:/home/hadoop/hadoop-dir/hadoop-2.5.1

echo " "
echo "Fitxers de configuracio replicats"
