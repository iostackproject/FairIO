#!/bin/bash

echo "Propagar test a hadoop100"
scp -r -P 2100 /home/beto/Test/ hadoop@localhost:/home/hadoop/hadoop-dir/Test

echo " "

