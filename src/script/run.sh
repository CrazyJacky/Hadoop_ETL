#!/bin/sh

INPUT=
OUPUT=
HDFS=hdfs://192.168.1.1:9000
JOB_TRACKER=192.168.1.1:9001

hadoop jar ../../target/hadoop-example.jar DRIVER -input input -output output.txt