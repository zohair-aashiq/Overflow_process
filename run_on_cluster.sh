#!/usr/bin/env bash
./compile.sh;
/home/oguz/Dev/spark-2.4.0-bin-hadoop2.7/bin/spark-submit --class Main --master spark://localhost:7077 Overflow-processor-assembly-0.1.jar --files app.conf