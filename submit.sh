#!/usr/bin/env bash

spark-submit \
  --class tasks.Main \
  --master spark://ws.gs:7077 \
  target/spark-examples-1.0-SNAPSHOT.jar