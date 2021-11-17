#!/usr/bin/env bash
./spark-submit.sh \
  --class example.spark.MultiBroadcastTest \
  --master spark://127.0.0.1:7077 \
  --deploy-mode cluster \
  --supervise \
  --executor-memory 1G \
  --total-executor-cores 2 \
  /Users/liujiage/IdeaProjects/spark_demo/target/sparkdemo-1.0-SNAPSHOT.jar