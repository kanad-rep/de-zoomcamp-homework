#!/bin/bash
# Run PySpark homework with Java 21+ compatibility fixes

export _JAVA_OPTIONS="-Djdk.lang.Process.launchMechanism=vfork"

spark-submit \
    --driver-java-options="-XX:+UnlockDiagnosticVMOptions -XX:+ShowMessageBoxOnError" \
    --conf spark.hadoop.fs.viewfs.impl.disable.cache=true \
    spark_homework.py
