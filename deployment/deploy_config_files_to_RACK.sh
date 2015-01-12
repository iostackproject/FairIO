#!/bin/bash

echo "Propagar conf a stacksync"
scp -r /home/beto/hadoop-2.5.1/conf-files-stacksync/hadoop/ hadoop@stacksync.urv.cat:/home/hadoop/hadoop-dir/hadoop-2.5.1/etc
scp -r /home/beto/hadoop-2.5.1/conf-files-stacksync/libexec/ hadoop@stacksync.urv.cat:/home/hadoop/hadoop-dir/hadoop-2.5.1/
scp -r /home/beto/hadoop-2.5.1/conf-files-stacksync/sbin/ hadoop@stacksync.urv.cat:/home/hadoop/hadoop-dir/hadoop-2.5.1/

echo " "
echo "Fitxers de configuracio replicats"
