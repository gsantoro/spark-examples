#!/usr/bin/env bash

export BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )"/.. && pwd )"

export SPARK_EXAMPLES="$BASE_DIR/target/spark-examples-1.0-SNAPSHOT.jar"

export LOG_CONFIG="log4j.properties"

spark-submit \
    --master local[4] \
    --class FirstNWords \
    --name FirstNWords \
    "$SPARK_EXAMPLES" \
    -Dlog4j.configuration="$LOG_CONFIG"
