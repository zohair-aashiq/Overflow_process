#!/usr/bin/env bash
echo "This script automates the compilation to a fat jar, pushing to cluster/HDFS and running the application";
echo "The specified app.conf must exist on "
./compile.sh;
ssh -t oguz@m5848.contaboserver.net  "hadoop fs -rm /user/oguz/Overflow-processor/processor.jar; sudo rm /home/oguz/overflow-processor/processor.jar";
scp Overflow-processor-assembly-0.1.jar oguz@m5848.contaboserver.net:/home/oguz/overflow-processor/processor.jar;
ssh -t oguz@m5848.contaboserver.net  "hadoop fs -put /home/oguz/overflow-processor/processor.jar\
    /user/oguz/Overflow-processor/processor.jar; tmux new \" spark-submit \
  --class Main \
  --master yarn \
  --deploy-mode cluster \
  --executor-memory 25G \
  --executor-cores 8\
  --conf spark.driver.memory=8G\
  --conf spark.blacklist.enabled=true\
  --conf spark.dynamicAllocation.enabled=true\
  --conf spark.dynamicAllocation.minExecutors=6\
  --conf spark.dynamicAllocation.maxExecutors=50\
  --conf spark.dynamicAllocation.schedulerBacklogTimeout=3s\
  --conf spark.dynamicAllocation.executorIdleTimeout=180s\
  --files hdfs:///user/oguz/whole/app.conf \
  hdfs:///user/oguz/Overflow-processor/processor.jar \" "

