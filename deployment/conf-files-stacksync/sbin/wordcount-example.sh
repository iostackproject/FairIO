#!/bin/bash
hdfs dfs -rm /output$1
hadoop jar ../share/hadoop/mapreduce/hadoop-mapreduce-examples-2.5.1.jar wordcount /input$1 /output$1
