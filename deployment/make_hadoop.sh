#!/bin/bash
echo 'Compilar i crear JAR'

mvn package -Pdist,src,native,tar -DskipTests # 2> make_errors.txt > make_output.txt

if [ $# -ne 0 ]
  then
    ./deploy_hadoop_to_VM.sh
fi

